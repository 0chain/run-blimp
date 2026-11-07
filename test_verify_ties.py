import unittest
import verify_ties as v


class ResolveOrderBy(unittest.TestCase):
    def test_source_column_projected_under_alias(self):
        # v_q55_cube: ORDER BY i_brand_id where the SELECT projects it AS brand_id.
        sql = ("SELECT i_brand_id AS brand_id, i_brand AS brand, SUM(ss_ext_sales_price) AS ext_price "
               "FROM date_dim, store_sales, item GROUP BY CUBE (i_brand, i_brand_id) "
               "ORDER BY ext_price DESC, i_brand_id LIMIT 100")
        self.assertEqual(v.resolve(["ext_price DESC", "i_brand_id"], ["brand_id", "brand", "ext_price"], sql), [2, 0])

    def test_unprojected_name_stays_unresolved(self):
        sql = "SELECT a AS x, SUM(b) AS s FROM t GROUP BY a ORDER BY s DESC, c LIMIT 10"
        self.assertIsNone(v.resolve(["s DESC", "c"], ["x", "s"], sql))


class _FakeFS:
    def __init__(self, lm):
        self.lm = lm

    def info(self, path):
        return {"LastModified": self.lm}


class WrittenSince(unittest.TestCase):
    def test_written_since_rejects_a_previous_runs_object(self):
        import datetime
        t = datetime.datetime(2026, 10, 25, 10, 45, 0, tzinfo=datetime.timezone.utc)
        sent = t.timestamp()
        # written after the request: fresh
        assert v.written_since(_FakeFS(t + datetime.timedelta(seconds=20)), "b/k", sent)
        # the previous run's object (minutes older): stale (node 37, v_q59_rollup)
        assert not v.written_since(_FakeFS(t - datetime.timedelta(minutes=5)), "b/k", sent)
        # no bound given: any object counts (old callers)
        assert v.written_since(_FakeFS(t - datetime.timedelta(days=1)), "b/k", 0)



class NonFinite(unittest.TestCase):
    # q59's week-over-week ratio puts inf in the tied rows (node 37, 2026-11-07)
    def test_literals_and_tolerance(self):
        self.assertEqual(v.sql_literal(float("inf")), "'inf'::DOUBLE")
        self.assertEqual(v.sql_literal(float("nan")), "'nan'::DOUBLE")
        m = v.membership_sql("SELECT a, b FROM t ORDER BY a LIMIT 3", (["a", "b"], [(1, float("inf"))]))
        self.assertIn("isfinite(q.c1) AND isfinite(v.c1)", m)

if __name__ == "__main__":
    unittest.main()



class KeyedProof(unittest.TestCase):
    """ORDER BY keys not projected (ClickBench v_q25_*): the page is proven by
    ranking the whole relation on the query's own ORDER BY."""
    Q = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 3;"
    SETUP = "CREATE TABLE hits AS SELECT * FROM (VALUES ('a',1),('b',2),('c',3),('d',3),('e',4),('', 0)) t(SearchPhrase, EventTime);"

    def test_not_applicable(self):
        for q in ["SELECT k, count(*) FROM t GROUP BY k ORDER BY max(x) LIMIT 3",
                  "SELECT DISTINCT k FROM t ORDER BY x LIMIT 3",
                  "SELECT k FROM t ORDER BY x LIMIT 3 OFFSET 2",
                  "SELECT k FROM a UNION ALL SELECT k FROM b ORDER BY 1 LIMIT 3",
                  # review K-1/K-2: an ordinal or a shadowing alias binds
                  # differently in a window; a comment swallows the statement
                  "SELECT SearchPhrase FROM hits ORDER BY 1, EventTime LIMIT 2",
                  "SELECT -a AS a FROM t ORDER BY a, b LIMIT 2",
                  "SELECT a FROM t -- x\nORDER BY b LIMIT 2",
                  # K-3..K-6: parenthesized ordinal / alias, quoted names, a
                  # repeated expression under two aliases
                  "SELECT a FROM t ORDER BY (1) DESC, x LIMIT 1",
                  "SELECT -a AS a FROM t ORDER BY (a) LIMIT 1",
                  'SELECT -a AS a FROM t ORDER BY "a", x LIMIT 1',
                  'SELECT -a AS "a" FROM t ORDER BY a, x LIMIT 1',
                  "SELECT -a AS x, -a AS a FROM t ORDER BY x DESC, b LIMIT 1"]:
            self.assertIsNone(v.keyed_proof_sql(q, (["k"], [("a",)])), q)

    def test_proof_in_duckdb(self):
        import shutil, subprocess
        if not shutil.which("duckdb"):
            self.skipTest("no duckdb CLI")
        def holds(served):
            want, stmt = v.keyed_proof_sql(self.Q, (["SearchPhrase"], served))
            out = subprocess.run(["duckdb", "-csv", "-noheader", "-c", self.SETUP + stmt + ";"], capture_output=True, text=True)
            self.assertEqual(out.returncode, 0, out.stderr)
            return len([l for l in out.stdout.splitlines() if l.strip()]) == want
        self.assertTrue(holds([("a",), ("b",), ("c",)]))
        self.assertTrue(holds([("a",), ("b",), ("d",)]))   # the other tied row
        self.assertFalse(holds([("a",), ("c",), ("d",)]))  # b ranks before the tie
        self.assertFalse(holds([("a",), ("b",), ("e",)]))  # e is past the page
        self.assertFalse(holds([("a",), ("b",)]))          # short page
        self.assertFalse(holds([("a",), ("a",), ("b",)]))  # a served twice
