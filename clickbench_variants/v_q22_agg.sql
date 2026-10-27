SELECT SearchPhrase, MIN(URL), COUNT(*) AS c, SUM(ResolutionWidth) AS sum_rw FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
