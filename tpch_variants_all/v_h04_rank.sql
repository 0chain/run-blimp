SELECT
  o_orderpriority,
  COUNT(*) AS order_count,
  RANK() OVER (ORDER BY COUNT(*) DESC) AS v_rank
FROM orders
WHERE
  o_orderdate >= CAST('1993-07-01' AS DATE)
  AND o_orderdate < CAST('1993-10-01' AS DATE)
  AND EXISTS(
    SELECT
      *
    FROM lineitem
    WHERE
      l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
  )
GROUP BY
  o_orderpriority
ORDER BY
  o_orderpriority
