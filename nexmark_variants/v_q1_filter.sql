SELECT auction, bidder, 0.908 * price AS price, dateTime, extra FROM bid WHERE price >= 10000 ORDER BY dateTime DESC, auction, bidder LIMIT 100;
