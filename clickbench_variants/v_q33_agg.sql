SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth), SUM(ResolutionWidth) AS sum_rw FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
