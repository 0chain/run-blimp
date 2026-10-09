SELECT
  SUM(CASE WHEN cs_ext_discount_amt > 0 THEN cs_ext_discount_amt ELSE 0 END) AS "excess discount amount"
FROM catalog_sales, item, date_dim
WHERE
  i_manufact_id = 977
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '2000-01-27' AND CAST('2000-04-26' AS DATE)
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt > (
    SELECT
      1.3 * AVG(cs_ext_discount_amt)
    FROM catalog_sales, date_dim
    WHERE
      cs_item_sk = i_item_sk
      AND d_date BETWEEN '2000-01-27' AND CAST('2000-04-26' AS DATE)
      AND d_date_sk = cs_sold_date_sk
  )
LIMIT 100
