SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE (SearchPhrase <> '') AND EventDate >= DATE '2013-07-10' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
