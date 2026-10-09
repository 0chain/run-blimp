SELECT
  SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE
  l_shipdate >= CAST('1995-01-01' AS DATE)
  AND l_shipdate < CAST('1996-01-01' AS DATE)
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
