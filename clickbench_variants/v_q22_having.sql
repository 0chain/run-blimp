SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase HAVING COUNT(*) > 10 ORDER BY c DESC LIMIT 10;
