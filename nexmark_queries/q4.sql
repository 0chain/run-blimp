-- Nexmark q4 (average price for a category): winning bid per auction, averaged per category
SELECT Q.category, AVG(Q.final) AS avg_final FROM (
  SELECT MAX(B.price) AS final, A.category FROM auction A, bid B
  WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires GROUP BY A.id, A.category) Q
GROUP BY Q.category ORDER BY Q.category;
