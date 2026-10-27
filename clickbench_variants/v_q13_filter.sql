SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE (SearchPhrase <> '') AND IsRefresh = 0 GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
