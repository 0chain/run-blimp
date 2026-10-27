-- Nexmark q20 (expand bid with auction): category-10 bids with their auction, newest 100
SELECT B.auction, B.bidder, B.price, B.channel, B.dateTime, A.itemName, A.initialBid, A.reserve, A.seller, A.category
FROM bid B JOIN auction A ON B.auction = A.id WHERE A.category = 10 ORDER BY B.dateTime DESC, B.auction, B.bidder LIMIT 100;
