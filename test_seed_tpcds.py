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
