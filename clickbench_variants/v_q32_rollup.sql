SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY ROLLUP(WatchID, ClientIP) ORDER BY c DESC LIMIT 10;
