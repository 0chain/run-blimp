SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE (URL LIKE '%google%' AND SearchPhrase <> '') AND IsRefresh = 0 GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
