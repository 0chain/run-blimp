SELECT
  l_orderkey,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue,
  o_orderdate,
  o_shippriority
FROM customer, orders, lineitem
WHERE
  c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < CAST('1995-03-15' AS DATE)
  AND l_shipdate > CAST('1995-03-15' AS DATE)
GROUP BY
  l_orderkey,
  o_orderdate,
  o_shippriority
HAVING
  COUNT(*) > 1
ORDER BY
  revenue DESC,
  o_orderdate
LIMIT 10
