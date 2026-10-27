-- Nexmark q9 (winning bids): each auction's highest in-window bid, newest auctions
SELECT id, itemName, seller, category, price, bidder FROM (
  SELECT A.id, A.itemName, A.seller, A.category, B.price, B.bidder,
         ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.dateTime ASC) AS rownum
  FROM auction A, bid B WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires) t
WHERE rownum <= 1 ORDER BY id DESC LIMIT 100;
