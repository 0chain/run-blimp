SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth), COUNT(DISTINCT UserID) AS users FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
