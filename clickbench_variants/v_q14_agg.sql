SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u, SUM(ResolutionWidth) AS sum_rw FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
