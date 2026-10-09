SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  SUM(CASE WHEN l_quantity > 0 THEN l_quantity ELSE 0 END)
FROM customer, orders, lineitem
WHERE
  o_orderkey IN (
    SELECT
      l_orderkey
    FROM lineitem
    GROUP BY
      l_orderkey
    HAVING
      SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
ORDER BY
  o_totalprice DESC,
  o_orderdate
LIMIT 100
