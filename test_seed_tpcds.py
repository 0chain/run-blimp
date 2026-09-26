#!/usr/bin/env python3
"""Regression tests for the CDC delta generator in seed_tpcds.py.

These guard the failure that invalidated the SF1000 CDC bench on test2
(2026-08-04): the appended delta carried NULL join keys and a date range outside
the MVs' year filters, so every "incremental merge" in the run computed an empty
result. q24's merge burned 15.2s scanning ~19.7 GB to produce a 0-row delta part;
q88's produced 0 rows; q4's left its MV at exactly 53,491,237 rows.

Pure-python only (no pyarrow / no catalog) so they run anywhere.

    python3 -m unittest test_seed_tpcds -v
"""
import random
import unittest

import seed_tpcds as S


# The join keys and measures each benched query needs to survive the append.
# If a column here is missing from the generated delta, conform_to_table_schema
# null-fills it and the query's delta aggregate is provably empty.
Q24_STORE_SALES = ["ss_ticket_number", "ss_item_sk", "ss_customer_sk",
                   "ss_store_sk", "ss_net_paid"]
Q24_STORE_RETURNS = ["sr_ticket_number", "sr_item_sk"]
Q88_STORE_SALES = ["ss_sold_time_sk", "ss_hdemo_sk", "ss_store_sk"]
Q4_STORE_SALES = ["ss_sold_date_sk", "ss_customer_sk", "ss_ext_list_price",
                  "ss_ext_wholesale_cost", "ss_ext_discount_amt", "ss_ext_sales_price"]


def gen(n=500, years=(2000, 2001, 2002), seed=1234):
    rnd = random.Random(seed)
    lo, hi = S.date_sk_bounds(list(years))
    pool = [rnd.randint(1, 50) for _ in range(n)]
    return S.gen_store_sales_cols(n, lo, hi, pool, rnd=rnd)


class TestDateBounds(unittest.TestCase):
    def test_single_year_matches_measured_date_dim(self):
        # Measured on test2's SF1000 date_dim, 2026-08-04.
        self.assertEqual(S.date_sk_bounds([2000]), (2451545, 2451910))
        self.assertEqual(S.date_sk_bounds([2001]), (2451911, 2452275))
        self.assertEqual(S.date_sk_bounds([2002]), (2452276, 2452640))

    def test_span_covers_every_requested_year(self):
        lo, hi = S.date_sk_bounds([2000, 2001, 2002])
        self.assertEqual((lo, hi), (2451545, 2452640))

    def test_q4_year_filter_is_reachable(self):
        """The bug: the old hardcoded 2000-only span could never satisfy q4.

        q4's MV body filters `d_year IN (2001, 2001+1)`. Every appended row has
        to be able to land in 2001..2002 or the merge is a guaranteed no-op."""
        q4_lo, q4_hi = S.date_sk_bounds([2001, 2002])
        old_lo, old_hi = 2451545, 2451910          # the shipped default
        self.assertTrue(old_hi < q4_lo, "regression fixture wrong: ranges must be disjoint")

        lo, hi = S.date_sk_bounds([2000, 2001, 2002])   # the new default
        self.assertTrue(lo <= q4_hi and hi >= q4_lo,
                        "default delta date span must overlap q4's d_year IN (2001,2002)")

    def test_unknown_year_is_rejected(self):
        with self.assertRaises(KeyError):
            S.date_sk_bounds([1975])


class TestStoreSalesDelta(unittest.TestCase):
    def test_every_benched_column_is_present_and_non_null(self):
        cols = gen()
        for q, needed in (("q24", Q24_STORE_SALES), ("q88", Q88_STORE_SALES),
                          ("q4", Q4_STORE_SALES)):
            for c in needed:
                with self.subTest(query=q, column=c):
                    self.assertIn(c, cols, f"{q} joins/aggregates on {c}")
                    self.assertTrue(all(v is not None for v in cols[c]),
                                    f"{c} must never be NULL — NULL kills the equi-join")

    def test_all_columns_same_length(self):
        cols = gen(n=137)
        self.assertTrue(all(len(v) == 137 for v in cols.values()))

    def test_ticket_numbers_are_unique_and_above_the_base_max(self):
        """New sales must not reuse an existing ticket, and the monotonic range
        is what lets parquet row-group min/max prune the full-table leg of an
        inclusion-exclusion delta."""
        cols = gen(n=1000)
        tickets = cols["ss_ticket_number"]
        self.assertEqual(len(set(tickets)), len(tickets), "ticket numbers must be unique")
        self.assertTrue(min(tickets) > 240_000_000,
                        "SF1000 base store_sales tops out at ss_ticket_number=240,000,000")
        self.assertEqual(tickets, sorted(tickets), "monotonic keys enable row-group pruning")

    def test_dates_land_inside_the_requested_years(self):
        for years in ([2000], [2001, 2002], [2000, 2001, 2002]):
            with self.subTest(years=years):
                lo, hi = S.date_sk_bounds(years)
                cols = gen(years=tuple(years))
                self.assertTrue(all(lo <= d <= hi for d in cols["ss_sold_date_sk"]))

    def test_net_paid_inc_tax_is_consistent(self):
        cols = gen(n=50)
        for paid, tax, inc in zip(cols["ss_net_paid"], cols["ss_ext_tax"],
                                  cols["ss_net_paid_inc_tax"]):
            self.assertEqual(inc, paid + tax)


class TestReferentialStoreReturns(unittest.TestCase):
    def test_returns_reference_the_sales_just_appended(self):
        """q24's join is store_sales x store_returns on (item_sk, ticket_number).

        Independently generated returns (the old behaviour) never match, so the
        merge scans both facts and yields nothing."""
        sales = gen(n=600)
        rets = S.gen_referential_store_returns_cols(sales, 200, rnd=random.Random(7))

        sale_pairs = set(zip(sales["ss_item_sk"], sales["ss_ticket_number"]))
        ret_pairs = set(zip(rets["sr_item_sk"], rets["sr_ticket_number"]))
        self.assertTrue(ret_pairs, "must emit at least one referential return")
        self.assertTrue(ret_pairs <= sale_pairs,
                        "every (item_sk, ticket_number) must reference an appended sale")

    def test_join_actually_produces_rows(self):
        """The end-to-end property the 15.2s merge failed: a non-empty join."""
        sales = gen(n=400)
        rets = S.gen_referential_store_returns_cols(sales, 150, rnd=random.Random(3))
        by_key = {(i, t): c for i, t, c in
                  zip(sales["ss_item_sk"], sales["ss_ticket_number"], sales["ss_customer_sk"])}
        matched = [k for k in zip(rets["sr_item_sk"], rets["sr_ticket_number"]) if k in by_key]
        self.assertEqual(len(matched), len(rets["sr_ticket_number"]),
                         "every referential return must join to its sale")

    def test_no_null_keys(self):
        sales = gen(n=300)
        rets = S.gen_referential_store_returns_cols(sales, 100, rnd=random.Random(9))
        for c in Q24_STORE_RETURNS:
            self.assertIn(c, rets)
            self.assertTrue(all(v is not None for v in rets[c]))

    def test_return_date_is_on_or_after_the_sale(self):
        sales = gen(n=200)
        rnd = random.Random(11)
        rets = S.gen_referential_store_returns_cols(sales, 80, rnd=rnd)
        sold = {t: d for t, d in zip(sales["ss_ticket_number"], sales["ss_sold_date_sk"])}
        for tkt, ret_date in zip(rets["sr_ticket_number"], rets["sr_returned_date_sk"]):
            self.assertGreater(ret_date, sold[tkt])

    def test_ratio_is_capped_at_the_sale_count(self):
        sales = gen(n=25)
        rets = S.gen_referential_store_returns_cols(sales, 999, rnd=random.Random(5))
        self.assertEqual(len(rets["sr_ticket_number"]), 25)

    def test_all_columns_same_length(self):
        sales = gen(n=90)
        rets = S.gen_referential_store_returns_cols(sales, 30, rnd=random.Random(2))
        self.assertTrue(all(len(v) == 30 for v in rets.values()))


class TestRegressionAgainstTheMeasuredFailure(unittest.TestCase):
    def test_old_column_set_would_have_failed_these_tests(self):
        """Documents exactly which omissions produced the 0-row merges."""
        old_store_sales = {
            "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_customer_sk",
            "ss_quantity", "ss_ext_sales_price", "ss_ext_wholesale_cost",
            "ss_net_profit", "ss_sales_price", "ss_list_price",
            "ss_ext_discount_amt", "ss_ext_list_price",
        }
        self.assertNotIn("ss_ticket_number", old_store_sales)   # q24 join key
        self.assertNotIn("ss_net_paid", old_store_sales)        # q24 measure
        self.assertNotIn("ss_sold_time_sk", old_store_sales)    # q88 join key
        self.assertNotIn("ss_hdemo_sk", old_store_sales)        # q88 join key

        cols = gen()
        for c in ("ss_ticket_number", "ss_net_paid", "ss_sold_time_sk", "ss_hdemo_sk"):
            self.assertIn(c, cols, f"{c} must now be generated")



# ===========================================================================
# The generic, schema-driven generator: every fact, every column.
#
# The store_sales-only tests above guard the specific columns that were measured
# empty. These guard the CLASS of bug: any column of any fact left unsynthesized
# gets null-filled and silently empties the delta of every query that reads it.
# ===========================================================================
FACTS = ["store_sales", "store_returns", "catalog_sales", "catalog_returns",
         "web_sales", "web_returns"]


def gen_any(fact, n=200, seed=42, dim_hi=None):
    rnd = random.Random(seed)
    lo, hi = S.date_sk_bounds([2000, 2001, 2002])
    return S.gen_fact_cols(fact, S.FACT_COLUMNS[fact], n, date_lo=lo, date_hi=hi,
                           dim_hi=dim_hi or {}, key_base=1, rnd=rnd)


class TestEveryFactIsFullyGenerated(unittest.TestCase):
    def test_every_physical_column_is_present_and_non_null(self):
        for fact in FACTS:
            cols = gen_any(fact)
            for name, _kind in S.FACT_COLUMNS[fact]:
                with self.subTest(fact=fact, column=name):
                    self.assertIn(name, cols)
                    self.assertTrue(all(v is not None for v in cols[name]))

    def test_no_column_is_constant_null_or_zero_width(self):
        for fact in FACTS:
            cols = gen_any(fact, n=137)
            for name, vals in cols.items():
                with self.subTest(fact=fact, column=name):
                    self.assertEqual(len(vals), 137)

    def test_web_returns_is_a_real_fact_not_a_store_returns_alias(self):
        """--table web_returns used to fall through to the store_returns branch,
        so the delta carried sr_* columns and EVERY wr_* column was null-filled:
        a 100%-dead delta for every web-returns query."""
        self.assertIn("web_returns", S.FACT_COLUMNS)
        cols = gen_any("web_returns")
        self.assertTrue(all(c.startswith("wr_") for c in cols))
        for c in ("wr_item_sk", "wr_order_number", "wr_return_amt",
                  "wr_returned_date_sk", "wr_net_loss", "wr_account_credit"):
            self.assertIn(c, cols)

    def test_every_query_referenced_column_exists(self):
        """Columns the 99-query TPC-DS set actually reads (scanned from
        ~/tpcds_queries on test2, 2026-08-04). Anything here that the generator
        omits is a silently-dropped join or aggregate."""
        needed = {
            "store_sales": ["ss_addr_sk", "ss_cdemo_sk", "ss_coupon_amt",
                            "ss_customer_sk", "ss_ext_discount_amt",
                            "ss_ext_list_price", "ss_ext_sales_price", "ss_ext_tax",
                            "ss_ext_wholesale_cost", "ss_hdemo_sk", "ss_item_sk",
                            "ss_list_price", "ss_net_paid", "ss_net_profit",
                            "ss_promo_sk", "ss_quantity", "ss_sales_price",
                            "ss_sold_date_sk", "ss_sold_time_sk", "ss_store_sk",
                            "ss_ticket_number", "ss_wholesale_cost"],
            "store_returns": ["sr_cdemo_sk", "sr_customer_sk", "sr_item_sk",
                              "sr_net_loss", "sr_reason_sk", "sr_return_amt",
                              "sr_return_quantity", "sr_returned_date_sk",
                              "sr_store_sk", "sr_ticket_number"],
            "catalog_sales": ["cs_bill_addr_sk", "cs_bill_cdemo_sk",
                              "cs_bill_customer_sk", "cs_bill_hdemo_sk",
                              "cs_call_center_sk", "cs_catalog_page_sk",
                              "cs_coupon_amt", "cs_ext_discount_amt",
                              "cs_ext_list_price", "cs_ext_sales_price",
                              "cs_ext_ship_cost", "cs_ext_wholesale_cost",
                              "cs_item_sk", "cs_list_price", "cs_net_paid",
                              "cs_net_paid_inc_tax", "cs_net_profit",
                              "cs_order_number", "cs_promo_sk", "cs_quantity",
                              "cs_sales_price", "cs_ship_addr_sk",
                              "cs_ship_customer_sk", "cs_ship_date_sk",
                              "cs_ship_mode_sk", "cs_sold_date_sk",
                              "cs_sold_time_sk", "cs_warehouse_sk",
                              "cs_wholesale_cost"],
            "catalog_returns": ["cr_call_center_sk", "cr_catalog_page_sk",
                                "cr_item_sk", "cr_net_loss", "cr_order_number",
                                "cr_refunded_cash", "cr_return_amount",
                                "cr_return_amt_inc_tax", "cr_return_quantity",
                                "cr_returned_date_sk", "cr_returning_addr_sk",
                                "cr_returning_customer_sk", "cr_reversed_charge",
                                "cr_store_credit"],
            "web_sales": ["ws_bill_addr_sk", "ws_bill_customer_sk",
                          "ws_ext_discount_amt", "ws_ext_list_price",
                          "ws_ext_sales_price", "ws_ext_ship_cost",
                          "ws_ext_wholesale_cost", "ws_item_sk", "ws_list_price",
                          "ws_net_paid", "ws_net_profit", "ws_order_number",
                          "ws_promo_sk", "ws_quantity", "ws_sales_price",
                          "ws_ship_addr_sk", "ws_ship_customer_sk",
                          "ws_ship_date_sk", "ws_ship_hdemo_sk", "ws_ship_mode_sk",
                          "ws_sold_date_sk", "ws_sold_time_sk", "ws_warehouse_sk",
                          "ws_web_page_sk", "ws_web_site_sk", "ws_wholesale_cost"],
            "web_returns": ["wr_fee", "wr_item_sk", "wr_net_loss", "wr_order_number",
                            "wr_reason_sk", "wr_refunded_addr_sk", "wr_refunded_cash",
                            "wr_refunded_cdemo_sk", "wr_return_amt",
                            "wr_return_quantity", "wr_returned_date_sk",
                            "wr_returning_addr_sk", "wr_returning_cdemo_sk",
                            "wr_returning_customer_sk", "wr_web_page_sk"],
        }
        for fact, cols_needed in needed.items():
            have = {c for c, _ in S.FACT_COLUMNS[fact]}
            for c in cols_needed:
                with self.subTest(fact=fact, column=c):
                    self.assertIn(c, have)

    def test_money_columns_are_internally_consistent(self):
        for fact in ("store_sales", "catalog_sales", "web_sales"):
            p = fact.split("_")[0][0] + "s_"
            c = gen_any(fact, n=60)
            for i in range(60):
                with self.subTest(fact=fact, row=i):
                    # ext_* = quantity * unit
                    self.assertEqual(c[p + "ext_sales_price"][i],
                                     S._q(c[p + "quantity"][i] * float(c[p + "sales_price"][i])))
                    # composites are EXACT sums of their quantized parts
                    self.assertEqual(c[p + "net_paid_inc_tax"][i],
                                     c[p + "net_paid"][i] + c[p + "ext_tax"][i])
                    self.assertEqual(c[p + "net_paid"][i],
                                     c[p + "ext_sales_price"][i] - c[p + "coupon_amt"][i])
                    self.assertEqual(c[p + "ext_discount_amt"][i],
                                     c[p + "ext_list_price"][i] - c[p + "ext_sales_price"][i])

    def test_measures_stay_inside_the_real_facts_value_ranges(self):
        """Ranges MEASURED off the real SF1 store_sales manifest bounds
        (2026-08-04). A delta whose values sit outside what the fact actually
        contains is invisible to every query with a literal value band —
        q13 filters ss_sales_price BETWEEN 50 AND 150, q28 filters list-price
        bands, q48 filters net-profit bands. A 3x markup put list_price at 299
        and ext_sales_price at 27,823, both off the end of the real distribution."""
        real = {"ss_quantity": (1, 100), "ss_wholesale_cost": (1, 100),
                "ss_list_price": (1, 200), "ss_sales_price": (0, 199.56),
                "ss_ext_sales_price": (0, 19308), "ss_ext_list_price": (1.06, 19984),
                "ss_net_profit": (-9969.53, 9407), "ss_net_paid": (0, 19443),
                "ss_ext_tax": (0, 1749.87), "ss_coupon_amt": (0, 17588.25)}
        cols = gen_any("store_sales", n=5000, seed=99)
        for c, (lo, hi) in real.items():
            vals = [float(v) for v in cols[c]]
            with self.subTest(column=c):
                self.assertGreaterEqual(min(vals), lo)
                self.assertLessEqual(max(vals), hi)

    def test_decimal_7_2_never_overflows(self):
        """Every measure is decimal(7,2): |v| must stay under 100000."""
        for fact in FACTS:
            cols = gen_any(fact, n=400)
            for name, kind in S.FACT_COLUMNS[fact]:
                if kind != "d":
                    continue
                for v in cols[name]:
                    with self.subTest(fact=fact, column=name):
                        self.assertLess(abs(v), 100000)


class TestDimensionKeyRanges(unittest.TestCase):
    """Every *_sk must resolve to a dimension and stay inside its real key range.

    A key above the dimension's max cannot join it: ss_promo_sk used
    randint(1,1800) while SF1000's promotion tops out at 1500 (~20% of the delta
    unjoinable), and the same randint(1,300000) for item drops ~94% at SF1 where
    item has only 18,000 rows."""

    def test_every_sk_column_maps_to_a_dimension(self):
        for fact in FACTS:
            for name, _k in S.FACT_COLUMNS[fact]:
                if not name.endswith("_sk"):
                    continue
                if name == S.FACT_KEY_COL.get(fact):
                    continue
                with self.subTest(fact=fact, column=name):
                    self.assertIsNotNone(S.dim_for(name),
                                         f"{name} has no dimension mapping — its "
                                         "range would be a blind guess")

    def test_demographics_suffixes_are_not_swallowed_by_customer_sk(self):
        self.assertEqual(S.dim_for("cs_bill_cdemo_sk")[0], "customer_demographics")
        self.assertEqual(S.dim_for("cs_bill_hdemo_sk")[0], "household_demographics")
        self.assertEqual(S.dim_for("cs_bill_customer_sk")[0], "customer")
        self.assertEqual(S.dim_for("cs_ship_addr_sk")[0], "customer_address")
        self.assertEqual(S.dim_for("wr_returning_cdemo_sk")[0], "customer_demographics")
        self.assertEqual(S.dim_for("ws_web_site_sk")[0], "web_site")
        self.assertEqual(S.dim_for("ws_web_page_sk")[0], "web_page")
        self.assertEqual(S.dim_for("cs_catalog_page_sk")[0], "catalog_page")

    def test_generated_keys_respect_the_catalog_supplied_maxima(self):
        """The whole point of reading bounds from the catalog: pass an SF1-sized
        dimension map and no generated key may exceed it."""
        sf1 = {"item": 18000, "customer": 100000, "customer_address": 50000,
               "customer_demographics": 1920800, "household_demographics": 7200,
               "store": 12, "promotion": 300, "call_center": 6, "catalog_page": 11718,
               "ship_mode": 20, "warehouse": 5, "web_page": 60, "web_site": 30,
               "reason": 35, "time_dim": 86399, "date_dim": 2488070}
        for fact in FACTS:
            cols = gen_any(fact, n=500, dim_hi=sf1)
            for name, _k in S.FACT_COLUMNS[fact]:
                d = S.dim_for(name)
                if not d or d[0] in ("date_dim", "time_dim"):
                    continue
                with self.subTest(fact=fact, column=name):
                    self.assertLessEqual(max(cols[name]), sf1[d[0]])
                    self.assertGreaterEqual(min(cols[name]), 1)

    def test_dims_needed_is_deduped_and_complete(self):
        need = S.dims_needed([c for c, _ in S.FACT_COLUMNS["catalog_sales"]])
        self.assertEqual(len(need), len(set(need)))
        for d in ("date_dim", "time_dim", "customer", "customer_demographics",
                  "household_demographics", "customer_address", "call_center",
                  "catalog_page", "ship_mode", "warehouse", "item", "promotion"):
            self.assertIn(d, need)


class TestKeysDoNotCollide(unittest.TestCase):
    """ss_ticket_number was a module CONSTANT and cs_order_number was
    random.randint(1e9, 9e9): append #2 reissued append #1's tickets, and the
    random order window overlapped the base's real range (max 8,859,306,610).
    Duplicate keys make the sales x returns join fan out — a WRONG delta, which
    is worse than an empty one because it looks like it worked."""

    def test_key_base_is_honoured_and_monotonic(self):
        for fact in ("store_sales", "catalog_sales", "web_sales"):
            keycol = S.FACT_KEY_COL[fact]
            rnd = random.Random(5)
            lo, hi = S.date_sk_bounds([2000])
            base = 8_859_306_611
            cols = S.gen_fact_cols(fact, S.FACT_COLUMNS[fact], 100, date_lo=lo,
                                   date_hi=hi, dim_hi={}, key_base=base, rnd=rnd)
            self.assertEqual(cols[keycol][0], base)
            self.assertEqual(cols[keycol], sorted(cols[keycol]))
            self.assertEqual(len(set(cols[keycol])), 100)

    def test_two_successive_appends_never_share_a_key(self):
        """Simulates what append_fact does: key_base = current max + 1."""
        fact, keycol = "store_sales", "ss_ticket_number"
        lo, hi = S.date_sk_bounds([2000])
        a1 = S.gen_fact_cols(fact, S.FACT_COLUMNS[fact], 50, date_lo=lo, date_hi=hi,
                             dim_hi={}, key_base=240_000_001, rnd=random.Random(1))
        nxt = max(a1[keycol]) + 1
        a2 = S.gen_fact_cols(fact, S.FACT_COLUMNS[fact], 50, date_lo=lo, date_hi=hi,
                             dim_hi={}, key_base=nxt, rnd=random.Random(2))
        self.assertFalse(set(a1[keycol]) & set(a2[keycol]))


class TestReferentialReturnsForEveryChannel(unittest.TestCase):
    def test_each_sales_fact_gets_matching_returns(self):
        for sales_fact, (rfact, keypairs) in S.RETURNS_OF.items():
            sales = gen_any(sales_fact, n=400)
            rets = S.gen_referential_returns(
                sales_fact, sales, S.FACT_COLUMNS[rfact], 120,
                date_lo=2451545, date_hi=2452640, dim_hi={}, rnd=random.Random(3))
            with self.subTest(fact=sales_fact):
                self.assertEqual(len(rets[keypairs[0][1]]), 120)
                sale_keys = set(zip(*[sales[s] for s, _ in keypairs]))
                ret_keys = set(zip(*[rets[r] for _, r in keypairs]))
                self.assertTrue(ret_keys, "must emit referential rows")
                self.assertTrue(ret_keys <= sale_keys,
                                f"{rfact} rows must reference appended {sales_fact}")

    def test_returns_carry_every_column_non_null(self):
        for sales_fact, (rfact, _kp) in S.RETURNS_OF.items():
            sales = gen_any(sales_fact, n=200)
            rets = S.gen_referential_returns(
                sales_fact, sales, S.FACT_COLUMNS[rfact], 60,
                date_lo=2451545, date_hi=2452640, dim_hi={}, rnd=random.Random(8))
            for name, _k in S.FACT_COLUMNS[rfact]:
                with self.subTest(fact=rfact, column=name):
                    self.assertIn(name, rets)
                    self.assertEqual(len(rets[name]), 60)
                    self.assertTrue(all(v is not None for v in rets[name]))

    def test_refund_stays_under_the_sale_so_q64s_having_band_holds(self):
        """q64's cs_ui keeps a group only when
        sum(cs_ext_list_price) > 2 * sum(refunded_cash+reversed_charge+store_credit)."""
        sales = gen_any("catalog_sales", n=1000)
        rets = S.gen_referential_returns(
            "catalog_sales", sales, S.FACT_COLUMNS["catalog_returns"], 100,
            date_lo=2451545, date_hi=2452640, dim_hi={}, rnd=random.Random(4))
        sale_total = sum(sales["cs_ext_list_price"])
        refund_total = sum(rets["cr_refunded_cash"]) + sum(rets["cr_reversed_charge"]) \
            + sum(rets["cr_store_credit"])
        self.assertGreater(sale_total, 2 * refund_total)

    def test_return_quantity_never_exceeds_the_sale(self):
        sales = gen_any("web_sales", n=300)
        rets = S.gen_referential_returns(
            "web_sales", sales, S.FACT_COLUMNS["web_returns"], 100,
            date_lo=2451545, date_hi=2452640, dim_hi={}, rnd=random.Random(6))
        by_order = dict(zip(sales["ws_order_number"], sales["ws_quantity"]))
        for o, q in zip(rets["wr_order_number"], rets["wr_return_quantity"]):
            self.assertLessEqual(q, by_order[o])
            self.assertGreaterEqual(q, 1)

    def test_returns_share_the_sales_dimension_keys(self):
        sales = gen_any("store_sales", n=300)
        rets = S.gen_referential_returns(
            "store_sales", sales, S.FACT_COLUMNS["store_returns"], 100,
            date_lo=2451545, date_hi=2452640, dim_hi={}, rnd=random.Random(10))
        pairs = dict(zip(sales["ss_ticket_number"], sales["ss_store_sk"]))
        for t, s in zip(rets["sr_ticket_number"], rets["sr_store_sk"]):
            self.assertEqual(s, pairs[t], "a return happens at the store that sold it")


class TestProportionalTick(unittest.TestCase):
    """Both drivers appended a FLAT count per table (50k to each of five facts on
    test2; 20k to three on AWS with returns omitted entirely). Real TPC-DS facts
    stand at roughly 4:2:1 store:catalog:web with returns ~10% of their parent."""

    def test_default_tick_matches_the_required_shape(self):
        plan = S.plan_tick(50000)
        self.assertEqual(
            plan,
            [("store_sales", 50000, "store_returns", 5000),
             ("catalog_sales", 25000, "catalog_returns", 2500),
             ("web_sales", 12500, "web_returns", 1250)])

    def test_all_six_facts_are_covered(self):
        touched = set()
        for sf, _sn, rf, _rn in S.plan_tick(50000):
            touched.add(sf); touched.add(rf)
        self.assertEqual(touched, set(FACTS))

    def test_ratio_is_configurable_not_hardcoded(self):
        plan = S.plan_tick(1000, S.parse_ratios("store_sales=1,catalog_sales=1,web_sales=1"),
                           returns_ratio=0.5)
        self.assertEqual([p[1] for p in plan], [1000, 1000, 1000])
        self.assertEqual([p[3] for p in plan], [500, 500, 500])

    def test_a_fact_can_be_dropped_from_the_tick(self):
        plan = S.plan_tick(1000, S.parse_ratios("store_sales=1,web_sales=0.25"))
        self.assertEqual([p[0] for p in plan], ["store_sales", "web_sales"])

    def test_returns_ratio_zero_means_sales_only(self):
        self.assertEqual([p[3] for p in S.plan_tick(1000, returns_ratio=0.0)],
                         [0, 0, 0])

    def test_unknown_fact_is_rejected(self):
        with self.assertRaises(ValueError):
            S.parse_ratios("inventory=1")

    def test_scales_with_base_rows(self):
        self.assertEqual([p[1] for p in S.plan_tick(20000)], [20000, 10000, 5000])


if __name__ == "__main__":
    unittest.main()


class TestGeoCorrelation(unittest.TestCase):
    """q24 requires `s_zip = ca_zip AND s_market_id = 8`. Drawing
    ss_customer_sk and ss_store_sk INDEPENDENTLY makes that pair essentially
    unreachable — measured on the SF1000 catalog (2026-08-04): 84 of 1002 stores
    are in market 8 covering 67 zips, 6.78% of the 6,000,000 addresses sit in one
    of those zips, so a random (customer, store) pair passes with p=8.46e-05.
    A 5,000-row store_returns delta expects 0.42 eligible rows, and q24's merge
    duly reported 11,408 ms over a 0-row delta part even after every column, date
    and dimension range had been fixed."""

    GEO = (  # (customer_sk, addr_sk, store_sk) triples that share a zip
        [11, 12, 13, 14], [110, 120, 130, 140], [7, 7, 9, 9])

    def test_triples_stay_together(self):
        cols = gen_any("store_sales", n=400)
        n = S.apply_geo_correlation(cols, "store_sales", self.GEO,
                                    rnd=random.Random(1))
        self.assertEqual(n, 400)
        allowed = set(zip(*self.GEO))
        for c, a, s in zip(cols["ss_customer_sk"], cols["ss_addr_sk"],
                           cols["ss_store_sk"]):
            self.assertIn((c, a, s), allowed,
                          "customer, address and store must come from ONE row of "
                          "the zip-consistent join, not be mixed across rows")

    def test_only_store_sales_is_correlated(self):
        for fact in ("catalog_sales", "web_sales", "store_returns"):
            cols = gen_any(fact, n=50)
            before = dict(cols)
            self.assertEqual(S.apply_geo_correlation(cols, fact, self.GEO), 0)
            self.assertEqual(cols, before)

    def test_disabled_geo_is_a_no_op(self):
        cols = gen_any("store_sales", n=50)
        before = dict(cols)
        self.assertEqual(S.apply_geo_correlation(cols, "store_sales", None), 0)
        self.assertEqual(cols, before)

    def test_other_columns_are_untouched(self):
        cols = gen_any("store_sales", n=200)
        keep = {c: list(v) for c, v in cols.items()
                if c not in ("ss_customer_sk", "ss_addr_sk", "ss_store_sk")}
        S.apply_geo_correlation(cols, "store_sales", self.GEO, rnd=random.Random(2))
        for c, v in keep.items():
            self.assertEqual(cols[c], v, f"{c} must keep its independent draw")


class StringDomainsTest(unittest.TestCase):
    """Appended dimension rows must not invent categories: categorical string
    columns draw from the live domain, id-like ones stay unique per row."""

    def test_categorical_strings_come_from_the_domain(self):
        import random
        cols = [("i_item_sk", "l"), ("i_item_id", "s"), ("i_category", "s"), ("i_current_price", "d")]
        out = S.gen_table_cols("item", cols, 40, date_lo=1, date_hi=2, dim_hi={}, key_base=1000,
                             rnd=random.Random(1), str_domains={"i_category": ["Books", "Music", "Shoes"]})
        self.assertTrue(set(out["i_category"]) <= {"Books", "Music", "Shoes"})
        self.assertEqual(len(set(out["i_item_id"])), 40)
        self.assertEqual(out["i_item_sk"], list(range(1000, 1040)))

    def test_without_domains_strings_are_unique_per_row(self):
        import random
        cols = [("i_item_sk", "l"), ("i_category", "s")]
        out = S.gen_table_cols("item", cols, 5, date_lo=1, date_hi=2, dim_hi={}, key_base=1, rnd=random.Random(1))
        self.assertEqual(out["i_category"][0], "i_category:1")


class TestDimBoundsAreScaleFree(unittest.TestCase):
    """The seeder must derive dimension key bounds from the DATA, never from a
    constant measured at one scale factor.

    Reported from a fresh on-prem node (2026-09-15): at SF1 the CDC tick logged
    "bounds unreadable; falling back to SF1000 value" and the delta-merge folded
    0 rows. The cause is not SF1 — it is that Iceberg only exposes bounds when
    the parquet carries column statistics, and without them the code guessed
    SF1000. Against the TPC-DS row counts that constant over-states `item` by
    94% at SF1, 66% at SF10 and 32% at SF100, so keys drawn above the real max
    cannot join. SF1 fails visibly; SF100 would fail QUIETLY with a plausible
    but wrong delta."""

    class _Col:
        """Minimal stand-in for an arrow column: only to_pylist() is needed."""
        def __init__(self, vals):
            self.vals = vals
        def to_pylist(self):
            return self.vals

    class _FakeCat:
        """Catalog whose manifests carry no statistics — the reported condition."""
        def __init__(self, maxes):
            self.maxes = maxes
            self.scanned = []

        def load_table(self, ident):
            table = ident[1]
            outer = self

            class _Arrow:
                def __init__(self, n):
                    self.num_rows = n
                def column(self, _c):
                    return outer.maxes[table]

            class _Scan:
                def to_arrow(self_inner):
                    outer.scanned.append(table)
                    return _Arrow(1)

            class _T:
                def scan(self_inner, selected_fields=None):
                    return _Scan()

            return _T()

    def _run(self, maxes):
        cat = self._FakeCat({k: self._Col([1, v, 2]) for k, v in maxes.items()})
        # catalog_bounds returns (None, None) here because _FakeCat has no
        # plan_files — exactly the stats-less case.
        return S.load_dim_hi(cat, "tpcds_sf1", list(maxes), verbose=False), cat

    def test_scans_the_real_max_when_stats_are_missing(self):
        for sf, maxes in (
            ("SF1", {"item": 18000, "customer": 100000, "store": 12}),
            ("SF10", {"item": 102000, "customer": 500000, "store": 102}),
            ("SF100", {"item": 204000, "customer": 2000000, "store": 402}),
        ):
            with self.subTest(sf=sf):
                out, cat = self._run(maxes)
                self.assertEqual(out, maxes, f"{sf}: must use the scanned max")
                self.assertEqual(sorted(cat.scanned), sorted(maxes),
                                 f"{sf}: every dimension must be scanned")

    def test_never_returns_the_sf1000_constant_when_data_is_readable(self):
        """The regression itself: SF1's item is 18000, not 300000."""
        out, _ = self._run({"item": 18000})
        self.assertEqual(out["item"], 18000)
        self.assertNotEqual(out["item"], S.FALLBACK_DIM_HI["item"],
                            "fell back to the SF1000 constant with data available")

    def test_constant_is_only_used_when_the_table_is_unreachable(self):
        class _Dead:
            def load_table(self, ident):
                raise RuntimeError("table unreachable")
        out = S.load_dim_hi(_Dead(), "tpcds_sf1", ["item"], verbose=False)
        self.assertEqual(out["item"], S.FALLBACK_DIM_HI["item"],
                         "a truly unreachable table still needs some bound")


# ---------------------------------------------------------------------------
# QUERY-TARGETED TICK (2026-09-19).
#
# The measured failure, from the gateway's own bounds prover on node
# 1788402989672 while ticking for q72:
#
#   kterm_dimfilter_empty: Δcatalog_sales.cs_sold_date_sk ∈ [2451545, 2452640]
#     vs date_dim.d_date_sk under "d1.d_year = 1999" ∈ [2451180, 2451544]
#     — disjoint, the term contributes no rows
#   delta_noop: the Δ terms produced 0 rows — no part written, watermark
#     advanced, MV untouched
#
# [2451545, 2452640] is exactly date_sk_bounds([2000, 2001, 2002]): the UNIFORM
# default. Every "merge" the run reported for q72 was a structural no-op, and
# the serve that followed (1,834 ms) proved nothing.
#
# These fixtures use the REAL q72 / q67 text (the predicates are the thing under
# test, so they are quoted verbatim, as test fixtures may).
# ---------------------------------------------------------------------------
import contextlib
import io
import json
import os
import tempfile

import query_pools as QP

Q72_SQL = """
SELECT i_item_desc, w_warehouse_name, d1.d_week_seq, count(*) total_cnt
FROM catalog_sales
JOIN inventory ON (cs_item_sk = inv_item_sk)
JOIN warehouse ON (w_warehouse_sk=inv_warehouse_sk)
JOIN item ON (i_item_sk = cs_item_sk)
JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT OUTER JOIN promotion ON (cs_promo_sk=p_promo_sk)
LEFT OUTER JOIN catalog_returns ON (cr_item_sk = cs_item_sk
                                    AND cr_order_number = cs_order_number)
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date + 5 -- SQL Server: DATEADD(day, 5, d1.d_date)
  AND hd_buy_potential = '>10000'
  AND d1.d_year = 1999
  AND cd_marital_status = 'D'
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
"""

Q67_SQL = """
SELECT i_category, d_year, s_store_id, sum(ss_sales_price*ss_quantity) sumsales
FROM store_sales, date_dim, store, item
WHERE ss_sold_date_sk=d_date_sk
  AND ss_item_sk=i_item_sk
  AND ss_store_sk = s_store_sk
  AND d_month_seq BETWEEN 1200 AND 1200+11
GROUP BY rollup(i_category, d_year, s_store_id)
"""

# The two windows the queries above actually select, from the SF1000 date_dim
# bounds already asserted by TestDateBounds. d_month_seq 1200..1211 is calendar
# year 2000.
W1999 = list(range(2451180, 2451545))
W2000 = list(range(2451545, 2451911))


class TestPoolDerivationBindsTheRightDateInstance(unittest.TestCase):
    """query_pools must say WHICH fact column each date window filters."""

    def test_q72_cross_alias_conjunct_is_not_a_filter(self):
        """`d3.d_date > d1.d_date + 5` compares two date_dim instances.

        It used to be filed as a third pool group under alias "" whose SQL
        (`d_date > d_date + 5`) is unsatisfiable; any such conjunct that DID
        return rows would union a non-filtering instance's keys into the
        filtering one's pool."""
        preds = QP.dim_predicates(Q72_SQL)
        self.assertEqual(sorted(preds["date_dim"]), ["d1"],
                         "only the d1 instance carries a filter")
        self.assertEqual(preds["date_dim"]["d1"], ["d_year = 1999"])

    def test_q72_binds_each_date_instance_to_its_fact_column(self):
        """The joins are in ON clauses, which where_conjuncts never sees."""
        self.assertEqual(QP.dim_join_columns(Q72_SQL, "date_dim"),
                         {"d1": ["cs_sold_date_sk"],
                          "d2": ["inv_date_sk"],
                          "d3": ["cs_ship_date_sk"]})

    def test_q67_binds_the_bare_date_dim_to_the_store_sales_date(self):
        self.assertEqual(QP.dim_join_columns(Q67_SQL, "date_dim"),
                         {"": ["ss_sold_date_sk"]})
        self.assertEqual(QP.dim_predicates(Q67_SQL)["date_dim"],
                         {"": ["d_month_seq BETWEEN 1200 AND 1200+11"]})

    def test_a_dimension_key_on_both_sides_is_not_a_fact_join(self):
        self.assertEqual(QP.dim_join_columns("select 1 from t where d_date_sk = d_date_sk",
                                             "date_dim"), {})


class TestPoolDerivationAgainstDuckDB(unittest.TestCase):
    """The predicates evaluated for real, against a date_dim fixture.

    Only the pool SQL is exercised here (no Iceberg / no catalog), which is what
    query_pools.pool_by_alias emits."""

    @classmethod
    def setUpClass(cls):
        try:
            import duckdb
        except ImportError:
            raise unittest.SkipTest("duckdb not installed")
        cls.con = duckdb.connect()
        cls.con.execute("ATTACH ':memory:' AS src; CREATE SCHEMA src.ns;")
        # d_date_sk 2450815 is 1998-01-01 and d_month_seq 1176 is 1998-01
        # (TPC-DS: month_seq counts months from 1900-01). One row per day,
        # 1998-01-01 .. 2002-12-31, generated — never a literal table.
        cls.con.execute("""
            CREATE TABLE src.ns.date_dim AS
            SELECT 2450815 + i           AS d_date_sk,
                   DATE '1998-01-01' + i::INTEGER AS d_date,
                   year(DATE '1998-01-01' + i::INTEGER) AS d_year,
                   1176 + (year(DATE '1998-01-01' + i::INTEGER) - 1998) * 12
                        + month(DATE '1998-01-01' + i::INTEGER) - 1 AS d_month_seq,
                   i / 7                 AS d_week_seq
            FROM range(0, 1826) t(i)
        """)

    def _pools(self, sql, name):
        preds = QP.dim_predicates(sql)
        joins = QP.dim_join_columns(sql, "date_dim")
        by_alias = QP.pool_by_alias(self.con, "ns", "date_dim", "d_date_sk",
                                    preds["date_dim"], 100000)
        out = {}
        for al, ks in by_alias.items():
            for c in joins.get(al, []):
                out[c] = ks
        return {"name": name, "date_by_col": out,
                "date_sk": sorted({k for v in out.values() for k in v}), "dims": {}}

    def test_q72_pool_is_1999_on_the_sold_date_only(self):
        p = self._pools(Q72_SQL, "q72")
        self.assertEqual(list(p["date_by_col"]), ["cs_sold_date_sk"])
        ks = p["date_by_col"]["cs_sold_date_sk"]
        self.assertEqual((min(ks), max(ks), len(ks)), (2451180, 2451544, 365),
                         "must be exactly the window the trace named")

    def test_q67_pool_is_2000_on_the_store_sales_date(self):
        p = self._pools(Q67_SQL, "q67")
        ks = p["date_by_col"]["ss_sold_date_sk"]
        self.assertEqual((min(ks), max(ks), len(ks)), (2451545, 2451910, 366))

    def test_the_two_windows_are_disjoint(self):
        """Which is why unioning them cannot serve both."""
        a = set(self._pools(Q72_SQL, "q72")["date_by_col"]["cs_sold_date_sk"])
        b = set(self._pools(Q67_SQL, "q67")["date_by_col"]["ss_sold_date_sk"])
        self.assertEqual(a & b, set())


def _pools_file(payload):
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(payload, f)
    return path


class PoolSeedingCase(unittest.TestCase):
    """Base: install pools, always restore, never leak into another test."""

    def setUp(self):
        self._saved = S.KEY_POOLS
        S.DATE_REPORTS.clear()

    def tearDown(self):
        S.KEY_POOLS = self._saved
        S.DATE_REPORTS.clear()

    def install(self, payload):
        path = _pools_file(payload)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                S.load_key_pools(path)
        finally:
            os.unlink(path)

    def gen(self, fact, n=400, seed=7):
        rnd = random.Random(seed)
        lo, hi = S.date_sk_bounds([2000, 2001, 2002])
        return S.gen_fact_cols(fact, S.FACT_COLUMNS[fact], n, date_lo=lo, date_hi=hi,
                               dim_hi={}, key_base=1, rnd=rnd)


# The wave under test: q72 (catalog_sales, 1999 + two demographics filters) and
# q67 (store_sales, 2000). CD/HD pools stand in for the keys
# `cd_marital_status='D'` / `hd_buy_potential='>10000'` select.
Q72_CD = list(range(1, 400))
Q72_HD = list(range(1, 25))
OTHER_CD = list(range(100000, 100400))       # some other query's demographics
WAVE = {
    "date_sk": sorted(W1999 + W2000),
    "dims": {"customer_demographics": sorted(Q72_CD + OTHER_CD),
             "household_demographics": Q72_HD},
    "date_by_col": {"cs_sold_date_sk": W1999, "ss_sold_date_sk": W2000},
    "queries": [
        {"name": "q72", "date_by_col": {"cs_sold_date_sk": W1999}, "date_sk": W1999,
         "dims": {"customer_demographics": Q72_CD, "household_demographics": Q72_HD}},
        {"name": "q67", "date_by_col": {"ss_sold_date_sk": W2000}, "date_sk": W2000,
         "dims": {}},
        {"name": "qX", "date_by_col": {}, "date_sk": [],
         "dims": {"customer_demographics": OTHER_CD}},
    ],
}


class TestTheMeasuredQ72Noop(PoolSeedingCase):
    def test_uniform_draws_land_entirely_OUTSIDE_the_q72_window(self):
        """Reproduce the failure before asserting the fix.

        No pools -> the uniform default span, which the trace showed is
        disjoint from d_year=1999."""
        self.install({})
        cols = self.gen("catalog_sales")
        sold = cols["cs_sold_date_sk"]
        self.assertEqual(sum(1 for v in sold if v in set(W1999)), 0,
                         "fixture wrong: the uniform span must miss 1999 entirely")
        self.assertGreaterEqual(min(sold), 2451545)
        self.assertLessEqual(max(sold), 2452640)

    def test_query_pools_put_every_catalog_sales_row_inside_the_q72_window(self):
        self.install(WAVE)
        cols = self.gen("catalog_sales")
        sold = cols["cs_sold_date_sk"]
        inwin = sum(1 for v in sold if v in set(W1999))
        self.assertEqual(inwin, len(sold),
                         "q72 is the only query constraining catalog_sales; all "
                         "its rows must land in 1999")

    def test_report_states_the_window_and_the_count(self):
        self.install(WAVE)
        S.DATE_REPORTS.clear()
        self.gen("catalog_sales")
        line = next(l for l in S.DATE_REPORTS if "cs_sold_date_sk" in l)
        self.assertIn("q72 [2451180..2451544]", line)
        self.assertIn("400/400 of the append in window", line)

    def test_report_names_the_noop_when_no_window_exists(self):
        self.install({})
        S.DATE_REPORTS.clear()
        self.gen("catalog_sales")
        line = next(l for l in S.DATE_REPORTS if "cs_sold_date_sk" in l)
        self.assertIn("(uniform)", line)
        self.assertIn("merges 0 of them", line)


class TestRowsAreCoherentWithOneQuery(PoolSeedingCase):
    """A row must satisfy ONE query's predicates TOGETHER, not a column each.

    q72's MV needs d_year=1999 AND cd_marital_status='D' AND
    hd_buy_potential='>10000' on the SAME row. Drawing each column from the
    wave-wide union independently satisfies each marginally and the conjunction
    almost never."""

    def _q72_rows(self, cols, datecol):
        cd, hd = set(Q72_CD), set(Q72_HD)
        win = set(W1999)
        return sum(1 for d, c, h in zip(cols[datecol], cols["cs_bill_cdemo_sk"],
                                        cols["cs_bill_hdemo_sk"])
                   if d in win and c in cd and h in hd)

    def test_union_pools_satisfy_the_conjunction_for_almost_no_row(self):
        """The old behaviour, as a legacy (queries-less) pools file."""
        self.install({k: WAVE[k] for k in ("date_sk", "dims")})
        cols = self.gen("catalog_sales", n=2000)
        hits = self._q72_rows(cols, "cs_sold_date_sk")
        # date 1/2 x cd 399/799 x hd 1 ~= 25%
        self.assertLess(hits, 0.4 * 2000,
                        "union draws should satisfy q72 on only a fraction of rows")

    def test_per_query_pools_satisfy_the_conjunction_for_every_owned_row(self):
        self.install(WAVE)
        cols = self.gen("catalog_sales", n=2000)
        hits = self._q72_rows(cols, "cs_sold_date_sk")
        owners = S.row_owners([c for c, _ in S.FACT_COLUMNS["catalog_sales"]], 2000)[0]
        owned = sum(1 for o in owners if o["name"] == "q72")
        self.assertEqual(hits, owned,
                         "every row owned by q72 must satisfy ALL of q72's predicates")
        self.assertGreater(hits, 0)

    def test_per_query_beats_union_on_the_same_seed(self):
        self.install({k: WAVE[k] for k in ("date_sk", "dims")})
        union_hits = self._q72_rows(self.gen("catalog_sales", n=2000), "cs_sold_date_sk")
        self.install(WAVE)
        pq_hits = self._q72_rows(self.gen("catalog_sales", n=2000), "cs_sold_date_sk")
        self.assertGreater(pq_hits, union_hits)


class TestEveryQueryGetsItsShare(PoolSeedingCase):
    def test_store_sales_rows_reach_q67s_window(self):
        """store_sales is claimed by q67 (its date) and by q72 (ss_cdemo_sk /
        ss_hdemo_sk), so q67 owns its round-robin share and every one of those
        rows is inside 2000."""
        self.install(WAVE)
        cols = self.gen("store_sales", n=600)
        names = [c for c, _ in S.FACT_COLUMNS["store_sales"]]
        owners = S.row_owners(names, 600)[0]
        win = set(W2000)
        q67 = [i for i, o in enumerate(owners) if o["name"] == "q67"]
        self.assertGreater(len(q67), 0, "q67 must own a share of store_sales")
        self.assertTrue(all(cols["ss_sold_date_sk"][i] in win for i in q67),
                        "every q67-owned row must be dated inside q67's window")

    def test_a_query_that_does_not_touch_the_fact_does_not_own_its_rows(self):
        self.install(WAVE)
        names = [c for c, _ in S.FACT_COLUMNS["catalog_sales"]]
        owners = set(o["name"] for o in S.row_owners(names, 50)[0])
        self.assertNotIn("q67", owners,
                         "q67 constrains only ss_sold_date_sk — it must not own "
                         "catalog_sales rows and date them in 2000")

    def test_owner_without_a_window_for_this_column_uses_its_own_other_window(self):
        """q72 has no window for ss_sold_date_sk. Its rows must NOT fall back to
        the wave-wide union (which contains q67's year) — its own 1999 window is
        the better answer and keeps the row self-consistent."""
        self.install(WAVE)
        cols = self.gen("store_sales", n=600)
        owners = S.row_owners([c for c, _ in S.FACT_COLUMNS["store_sales"]], 600)[0]
        win = set(W1999)
        q72 = [i for i, o in enumerate(owners) if o["name"] == "q72"]
        self.assertTrue(all(cols["ss_sold_date_sk"][i] in win for i in q72))


class TestShipDatesStillTrailTheSale(PoolSeedingCase):
    def test_ship_after_sold_with_pools(self):
        self.install(WAVE)
        cols = self.gen("catalog_sales", n=500)
        self.assertTrue(all(s < sh for s, sh in zip(cols["cs_sold_date_sk"],
                                                    cols["cs_ship_date_sk"])),
                        "q72 itself requires d3.d_date > d1.d_date + 5")

    def test_a_ship_window_is_honoured_but_never_before_the_sale(self):
        """A query filtering the SHIP date gets its own window for that column."""
        ship_win = list(range(2451200, 2451500))
        self.install({"date_sk": W1999, "dims": {},
                      "date_by_col": {"cs_sold_date_sk": W1999,
                                      "cs_ship_date_sk": ship_win},
                      "queries": [{"name": "qS",
                                   "date_by_col": {"cs_sold_date_sk": W1999,
                                                   "cs_ship_date_sk": ship_win},
                                   "date_sk": W1999, "dims": {}}]})
        cols = self.gen("catalog_sales", n=500)
        pairs = list(zip(cols["cs_sold_date_sk"], cols["cs_ship_date_sk"]))
        self.assertTrue(all(sh > s for s, sh in pairs), "ship must trail the sale")
        inwin = sum(1 for _, sh in pairs if sh in set(ship_win))
        self.assertGreater(inwin, 0.5 * len(pairs),
                           "most ship dates should land in the query's ship window")


class TestNonFactTablesUseTheSameWindows(PoolSeedingCase):
    def test_inventory_date_takes_the_window_bound_to_its_own_column(self):
        """q72's d2 is joined to inv_date_sk. If that instance ever carries a
        filter, the inventory append must honour IT — not the sold-date pool."""
        inv_win = list(range(2451300, 2451340))
        self.install({"date_sk": W1999, "dims": {},
                      "date_by_col": {"inv_date_sk": inv_win},
                      "queries": [{"name": "q72", "date_by_col": {"inv_date_sk": inv_win},
                                   "date_sk": inv_win, "dims": {}}]})
        cols = S.gen_table_cols(
            "inventory",
            [("inv_date_sk", "i"), ("inv_item_sk", "i"), ("inv_warehouse_sk", "i"),
             ("inv_quantity_on_hand", "i")],
            300, date_lo=2451545, date_hi=2452640, dim_hi={}, key_base=1,
            rnd=random.Random(3))
        self.assertTrue(all(v in set(inv_win) for v in cols["inv_date_sk"]))


class TestStreamModeStillIgnoresPools(PoolSeedingCase):
    def test_clear_key_pools_drops_every_pool(self):
        self.install(WAVE)
        S.clear_key_pools()
        self.assertEqual(S.KEY_POOLS,
                         {"date_sk": [], "dims": {}, "date_by_col": {}, "queries": []})
        cols = self.gen("catalog_sales", n=100)
        self.assertEqual(sum(1 for v in cols["cs_sold_date_sk"] if v in set(W1999)), 0)


class TestLegacyPoolsFileStillWorks(PoolSeedingCase):
    def test_a_file_with_no_queries_key_behaves_as_before(self):
        self.install({"date_sk": W1999, "dims": {"item": [1, 2, 3]}})
        cols = self.gen("catalog_sales", n=200)
        self.assertTrue(all(v in set(W1999) for v in cols["cs_sold_date_sk"]))
        self.assertTrue(all(v in {1, 2, 3} for v in cols["cs_item_sk"]))


class TestEndToEndQueryTextToAppendedRows(PoolSeedingCase):
    """q72 + q67 SQL -> query_pools.main() -> pools JSON -> appended rows.

    The whole chain, with only the Iceberg connection stubbed by the same
    date_dim fixture TestPoolDerivationAgainstDuckDB builds. This is the test
    that would have caught the measured no-op."""

    def setUp(self):
        super().setUp()
        try:
            import duckdb
        except ImportError:
            raise unittest.SkipTest("duckdb not installed")
        self.tmp = tempfile.mkdtemp()
        for name, sql in (("q72", Q72_SQL), ("q67", Q67_SQL)):
            with open(os.path.join(self.tmp, name + ".sql"), "w") as f:
                f.write(sql)
        self.con = duckdb.connect()
        self.con.execute("ATTACH ':memory:' AS src; CREATE SCHEMA src.ns;")
        self.con.execute("""
            CREATE TABLE src.ns.date_dim AS
            SELECT 2450815 + i AS d_date_sk,
                   DATE '1998-01-01' + i::INTEGER AS d_date,
                   year(DATE '1998-01-01' + i::INTEGER) AS d_year,
                   1176 + (year(DATE '1998-01-01' + i::INTEGER) - 1998) * 12
                        + month(DATE '1998-01-01' + i::INTEGER) - 1 AS d_month_seq
            FROM range(0, 1826) t(i)
        """)
        # the demographics tables q72 also filters
        self.con.execute("CREATE TABLE src.ns.customer_demographics AS "
                         "SELECT i AS cd_demo_sk, CASE WHEN i%4=0 THEN 'D' ELSE 'M' END "
                         "AS cd_marital_status FROM range(1, 4001) t(i)")
        self.con.execute("CREATE TABLE src.ns.household_demographics AS "
                         "SELECT i AS hd_demo_sk, CASE WHEN i%10=0 THEN '>10000' ELSE '501-1000' END "
                         "AS hd_buy_potential FROM range(1, 1001) t(i)")

    def _derive(self):
        import sys
        out = os.path.join(self.tmp, "pools.json")
        argv = sys.argv
        connect = QP.duck_connect
        QP.duck_connect = lambda a: self.con
        sys.argv = ["query_pools.py",
                    "--sql-file", os.path.join(self.tmp, "q72.sql"),
                    "--sql-file", os.path.join(self.tmp, "q67.sql"),
                    "--catalog", "x", "--warehouse", "x", "--namespace", "ns",
                    "--out", out]
        try:
            with contextlib.redirect_stderr(io.StringIO()) as err:
                QP.main()
        finally:
            sys.argv = argv
            QP.duck_connect = connect
        with open(out) as f:
            return json.load(f), err.getvalue()

    def test_pools_json_carries_a_per_query_window_per_fact_column(self):
        pools, _ = self._derive()
        byq = {q["name"]: q for q in pools["queries"]}
        self.assertEqual(sorted(byq), ["q67", "q72"])
        self.assertEqual(list(byq["q72"]["date_by_col"]), ["cs_sold_date_sk"])
        ks = byq["q72"]["date_by_col"]["cs_sold_date_sk"]
        self.assertEqual((min(ks), max(ks)), (2451180, 2451544))
        ks = byq["q67"]["date_by_col"]["ss_sold_date_sk"]
        self.assertEqual((min(ks), max(ks)), (2451545, 2451910))
        # q72's demographics predicates came through too, and are ITS keys
        self.assertTrue(all(k % 4 == 0 for k in byq["q72"]["dims"]["customer_demographics"]))
        self.assertTrue(all(k % 10 == 0 for k in byq["q72"]["dims"]["household_demographics"]))

    def test_the_stderr_log_names_the_instance_the_window_binds_to(self):
        _, log = self._derive()
        self.assertIn("date_dim [d1] -> cs_sold_date_sk: 365 key(s) [2451180..2451544]", log)
        self.assertIn("date_dim [-] -> ss_sold_date_sk: 366 key(s) [2451545..2451910]", log)

    def test_appended_catalog_sales_rows_satisfy_every_q72_predicate(self):
        pools, _ = self._derive()
        self.install(pools)
        cols = self.gen("catalog_sales", n=1000)
        win = set(pools["queries"][0]["date_by_col"]["cs_sold_date_sk"])
        cd = set(pools["queries"][0]["dims"]["customer_demographics"])
        hd = set(pools["queries"][0]["dims"]["household_demographics"])
        self.assertEqual(pools["queries"][0]["name"], "q72")
        hits = sum(1 for d, c, h in zip(cols["cs_sold_date_sk"],
                                        cols["cs_bill_cdemo_sk"],
                                        cols["cs_bill_hdemo_sk"])
                   if d in win and c in cd and h in hd)
        self.assertEqual(hits, 1000,
                         "q72 is the only query constraining catalog_sales; every "
                         "appended row must satisfy its year AND both demographics")

    def test_without_the_pools_the_same_rows_satisfy_none_of_them(self):
        """The before picture, on the same generator and the same seed."""
        pools, _ = self._derive()
        self.install({})
        cols = self.gen("catalog_sales", n=1000)
        win = set(pools["queries"][0]["date_by_col"]["cs_sold_date_sk"])
        self.assertEqual(sum(1 for d in cols["cs_sold_date_sk"] if d in win), 0)


class DimOwnKeyResolvesTest(unittest.TestCase):
    """A dimension's own surrogate key must be found when appending to it.

    dim_for() resolves FACT columns by suffix, and a dimension's own key does
    not carry that suffix ("hd_demo_sk" ends in "_demo_sk", not "hdemo_sk").
    When append_table() could not name the key it skipped the whole bounds
    block and issued keys from 1, so every tick re-used existing keys: measured
    on the node 2026-09-21, household_demographics held 62,320 rows over 7,200
    distinct hd_demo_sk. A re-used key is reachable from pre-append facts, so
    the RI-prune gate refuses the dim delta and the merge pays a fact scan.
    """

    def _keycol(self, table, columns):
        keycol = next((c for c, _ in columns if (S.dim_for(c) or ("",))[0] == table), None)
        if keycol is None:
            own = S.own_key_of(table)
            if own and any(c == own for c, _ in columns):
                keycol = own
        return keycol

    def test_demographics_dims_resolve_their_own_key(self):
        for table, key in (("household_demographics", "hd_demo_sk"),
                           ("customer_demographics", "cd_demo_sk")):
            cols = [(key, "int"), ("x_other", "string")]
            self.assertIsNone(S.dim_for(key), f"{key} should not resolve as a fact column")
            self.assertEqual(self._keycol(table, cols), key,
                             f"{table}: own key not resolved — appends would re-issue keys from 1")

    def test_suffix_matching_dims_still_resolve(self):
        for table, key in (("item", "i_item_sk"), ("warehouse", "w_warehouse_sk"),
                           ("date_dim", "d_date_sk")):
            self.assertEqual(self._keycol(table, [(key, "int")]), key)

    def test_non_dimension_table_has_no_own_key(self):
        self.assertIsNone(self._keycol("catalog_sales", [("cs_quantity", "int")]))


class EveryAppendedDimIssuesFreshKeysTest(unittest.TestCase):
    """Every dimension bench_cdc.sh appends to must issue its own key above the
    table's max. income_band's ib_income_band_sk matched no DIM_BY_SUFFIX entry,
    so own_key_of() was None, append_table() skipped its bounds lookup and
    gen_table_cols() filled the PK with randint(1, 1000): measured on nodes 37
    and 144 (2026-09-25) income_band held 56,352 / 10,144 live rows over 1,000
    distinct keys. The table list is read from bench_cdc.sh's default
    CDC_EXTRA_TABLES, so a dimension added there is covered here too."""

    def _extra_tables(self):
        import os, re
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_cdc.sh")).read()
        m = re.search(r'EXTRA_TABLES="\$\{CDC_EXTRA_TABLES-([^}]*)\}"', src)
        self.assertIsNotNone(m, "bench_cdc.sh default CDC_EXTRA_TABLES not found")
        return m.group(1).split()

    def test_every_appended_dimension_has_an_own_key(self):
        for table in self._extra_tables():
            if table == "inventory":      # composite key (date, item, warehouse): no surrogate
                continue
            with self.subTest(table=table):
                key = S.own_key_of(table)
                self.assertIsNotNone(key, f"{table}: no own key — appends re-use existing keys")
                cols = S.gen_table_cols(table, [(key, "i")], 6, date_lo=2451180,
                                        date_hi=2451544, dim_hi={}, key_base=5001)
                self.assertEqual(cols[key], list(range(5001, 5007)))

    def test_income_band_fk_joins_income_band(self):
        self.assertEqual(S.dim_for("hd_income_band_sk"), ("income_band", "ib_income_band_sk"))
        cols = S.gen_table_cols("household_demographics",
                                [("hd_demo_sk", "i"), ("hd_income_band_sk", "i")], 200,
                                date_lo=2451180, date_hi=2451544,
                                dim_hi={"income_band": 20}, key_base=1)
        self.assertLessEqual(max(cols["hd_income_band_sk"]), 20)


class DimOwnPKIsIssuedAboveMaxTest(unittest.TestCase):
    """gen_table_cols must fill a dimension's own PK from key_base, not noise.

    The own-PK branch asked dim_for(name), which resolves FACT columns; for
    hd_demo_sk / cd_demo_sk it returned None, so the PK fell through to the
    integer-noise branch. Measured on the node 2026-09-21: 6-row household_
    demographics tick appends carried random hd_demo_sk in 25..943 while the
    append printed "issuing 7201..7206".
    """

    def test_own_key_of_resolves_demographics(self):
        self.assertEqual(S.own_key_of("household_demographics"), "hd_demo_sk")
        self.assertEqual(S.own_key_of("customer_demographics"), "cd_demo_sk")
        self.assertEqual(S.own_key_of("item"), "i_item_sk")
        self.assertIsNone(S.own_key_of("catalog_sales"))

    def test_generated_pk_is_contiguous_from_key_base(self):
        for table, key in (("household_demographics", "hd_demo_sk"),
                           ("customer_demographics", "cd_demo_sk"),
                           ("item", "i_item_sk")):
            cols = S.gen_table_cols(table, [(key, "i")], 6,
                                    date_lo=2451180, date_hi=2451544,
                                    dim_hi={}, key_base=7201)
            self.assertEqual(cols[key], list(range(7201, 7207)),
                             f"{table}.{key} not issued from key_base — appends would re-use keys")
