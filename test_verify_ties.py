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


if __name__ == "__main__":
    unittest.main()
