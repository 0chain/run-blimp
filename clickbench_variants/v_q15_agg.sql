SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c, SUM(ResolutionWidth) AS sum_rw FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
