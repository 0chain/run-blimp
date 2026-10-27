SELECT SearchEngineID, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID ORDER BY c DESC LIMIT 10;
