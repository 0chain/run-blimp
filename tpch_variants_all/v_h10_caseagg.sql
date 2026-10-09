SELECT
  c_custkey,
  c_name,
  SUM(
    CASE
      WHEN l_extendedprice * (
        1 - l_discount
      ) > 0
      THEN l_extendedprice * (
        1 - l_discount
      )
      ELSE 0
    END
  ) AS revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
FROM customer, orders, lineitem, nation
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= CAST('1993-10-01' AS DATE)
  AND o_orderdate < CAST('1994-01-01' AS DATE)
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY
  c_custkey,
  c_name,
  c_acctbal,
  c_phone,
  n_name,
  c_address,
  c_comment
ORDER BY
  revenue DESC
LIMIT 20
