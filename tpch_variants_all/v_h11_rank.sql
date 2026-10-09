SELECT
  ps_partkey,
  SUM(ps_supplycost * ps_availqty) AS value,
  RANK() OVER (ORDER BY SUM(ps_supplycost * ps_availqty) DESC) AS v_rank
FROM partsupp, supplier, nation
WHERE
  ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
GROUP BY
  ps_partkey
HAVING
  SUM(ps_supplycost * ps_availqty) > (
    SELECT
      SUM(ps_supplycost * ps_availqty) * 0.0001000000
    FROM partsupp, supplier, nation
    WHERE
      ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
  )
ORDER BY
  value DESC
