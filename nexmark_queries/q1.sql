-- Nexmark q1 (currency conversion), batch form: the newest 100 bids, prices in euros
SELECT auction, bidder, 0.908 * price AS price, dateTime, extra FROM bid ORDER BY dateTime DESC, auction, bidder LIMIT 100;
