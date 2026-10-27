-- Nexmark q6 (average selling price by seller): mean of each seller's last 10 winning bids (q9's winning bid, reserve met)
SELECT seller, AVG(final) AS avg_final FROM (
  SELECT seller, price AS final, ROW_NUMBER() OVER (PARTITION BY seller ORDER BY expires DESC, id DESC) AS rn FROM (
    SELECT A.id, A.seller, A.expires, B.price,
           ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
    FROM auction A, bid B WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires AND B.price >= A.reserve) w
  WHERE rownum = 1) t
WHERE rn <= 10 GROUP BY seller ORDER BY avg_final DESC, seller LIMIT 100;
