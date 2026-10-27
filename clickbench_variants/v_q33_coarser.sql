SELECT WatchID, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID ORDER BY c DESC LIMIT 10;
