-- Nexmark q5 (hot items): bids per auction over sliding windows (10 s size, 2 s slide), each window's top auctions
WITH hb AS (
  SELECT auction, time_bucket(INTERVAL 2 SECOND, dateTime) - INTERVAL (s * 2) SECOND AS starttime
  FROM bid, (SELECT unnest(range(0, 5)) AS s) k),
c AS (SELECT auction, starttime, count(*) AS num FROM hb GROUP BY auction, starttime)
SELECT c.auction, c.num, c.starttime FROM c JOIN (SELECT starttime, max(num) AS maxn FROM c GROUP BY starttime) m
ON c.starttime = m.starttime AND c.num >= m.maxn ORDER BY c.starttime DESC, c.auction LIMIT 100;
