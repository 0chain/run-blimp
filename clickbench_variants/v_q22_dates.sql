SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE (URL LIKE '%google%' AND SearchPhrase <> '') AND EventDate >= DATE '2013-07-10' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
