SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY ROLLUP(WatchID, ClientIP) ORDER BY c DESC LIMIT 10;
