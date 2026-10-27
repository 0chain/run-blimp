SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase HAVING COUNT(*) > 10 ORDER BY c DESC LIMIT 10;
