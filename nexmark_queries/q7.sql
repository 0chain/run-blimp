-- Nexmark q7 (highest bid): the top bid of each 10 s window
SELECT B.auction, B.price, B.bidder, B.dateTime FROM bid B JOIN (
  SELECT MAX(price) AS maxprice, time_bucket(INTERVAL 10 SECOND, dateTime) AS w FROM bid GROUP BY w) M
ON time_bucket(INTERVAL 10 SECOND, B.dateTime) = M.w AND B.price = M.maxprice
ORDER BY B.dateTime DESC, B.auction LIMIT 100;
