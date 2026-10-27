SELECT auction, bidder, 0.908 * price AS price, dateTime, extra FROM bid WHERE dateTime >= TIMESTAMP '2015-07-16 00:00:00' ORDER BY dateTime DESC, auction, bidder LIMIT 100;
