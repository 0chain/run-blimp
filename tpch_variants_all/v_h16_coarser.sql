SELECT
  p_type,
  p_size,
  COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE
  p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND NOT p_type LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND NOT ps_suppkey IN (
    SELECT
      s_suppkey
    FROM supplier
    WHERE
      s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY
  p_type,
  p_size
ORDER BY
  supplier_cnt DESC,
  p_type,
  p_size
