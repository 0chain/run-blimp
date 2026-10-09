SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(CASE WHEN l_extendedprice > 0 THEN l_extendedprice ELSE 0 END) AS sum_base_price,
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS sum_disc_price,
  SUM(l_extendedprice * (
    1 - l_discount
  ) * (
    1 + l_tax
  )) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE
  l_shipdate <= CAST('1998-09-02' AS DATE)
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus
