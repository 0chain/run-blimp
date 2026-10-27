SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY ROLLUP(SearchEngineID, SearchPhrase) ORDER BY c DESC LIMIT 10;
