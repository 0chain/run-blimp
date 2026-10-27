SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE (SearchPhrase <> '') AND IsRefresh = 0 GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
