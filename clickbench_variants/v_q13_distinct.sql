SELECT SearchPhrase, COUNT(*) AS c, COUNT(DISTINCT UserID) AS users FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
