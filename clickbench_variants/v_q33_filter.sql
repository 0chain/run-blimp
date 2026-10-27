SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE IsRefresh = 0 GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
