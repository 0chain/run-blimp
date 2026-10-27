SELECT SearchPhrase, MIN(URL), COUNT(*) AS c, COUNT(DISTINCT UserID) AS users FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
