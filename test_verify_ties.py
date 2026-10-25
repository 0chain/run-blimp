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


if __name__ == "__main__":
    unittest.main()

