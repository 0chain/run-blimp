SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ROLLUP(ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3) ORDER BY c DESC LIMIT 10;
