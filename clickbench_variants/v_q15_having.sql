SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase HAVING COUNT(*) > 10 ORDER BY c DESC LIMIT 10;
