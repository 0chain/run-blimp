-- Nexmark q19 (auction top-10 price): each auction's 10 highest bids; the highest-priced 100 rows
SELECT auction, bidder, price, rank_number FROM (
  SELECT auction, bidder, price, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, bidder, dateTime) AS rank_number FROM bid) t
WHERE rank_number <= 10 ORDER BY price DESC, auction, rank_number LIMIT 100;
