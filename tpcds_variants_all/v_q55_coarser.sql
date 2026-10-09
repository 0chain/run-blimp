SELECT
  i_brand AS brand,
  SUM(ss_ext_sales_price) AS ext_price
FROM date_dim, store_sales, item
WHERE
  d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 28
  AND d_moy = 11
  AND d_year = 1999
GROUP BY
  i_brand
ORDER BY
  ext_price DESC
LIMIT 100
