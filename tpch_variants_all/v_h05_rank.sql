SELECT
  n_name,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue,
  RANK() OVER (ORDER BY SUM(l_extendedprice * (
    1 - l_discount
  )) DESC) AS v_rank
FROM customer, orders, lineitem, supplier, nation, region
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= CAST('1994-01-01' AS DATE)
  AND o_orderdate < CAST('1995-01-01' AS DATE)
GROUP BY
  n_name
ORDER BY
  revenue DESC
