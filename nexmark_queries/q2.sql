-- Nexmark q2 (selection): bids on every 123rd auction, the 100 latest auctions' highest bids
SELECT auction, price FROM bid WHERE auction % 123 = 0 ORDER BY auction DESC, price DESC LIMIT 100;
