-- Nexmark q18 (last bid per bidder and auction)
SELECT auction, bidder, price, dateTime FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number FROM bid) t
WHERE rank_number <= 1 ORDER BY dateTime DESC, auction, bidder LIMIT 100;
