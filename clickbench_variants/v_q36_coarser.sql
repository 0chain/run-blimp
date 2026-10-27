SELECT ClientIP, ClientIP - 1, ClientIP - 2, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2 ORDER BY c DESC LIMIT 10;
