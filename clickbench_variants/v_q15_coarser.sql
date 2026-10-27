SELECT SearchEngineID, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID ORDER BY c DESC LIMIT 10;
