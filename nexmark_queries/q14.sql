-- Nexmark q14 (calculation): bids between 1M and 50M by time of day, newest 100
SELECT auction, bidder, 0.908 * price AS price,
  CASE WHEN hour(dateTime) >= 8 AND hour(dateTime) <= 18 THEN 'dayTime' WHEN hour(dateTime) <= 6 OR hour(dateTime) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bidTimeType,
  dateTime, length(extra) - length(replace(extra, 'c', '')) AS c_counts
FROM bid WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000 ORDER BY dateTime DESC, auction, bidder LIMIT 100;
