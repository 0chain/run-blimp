SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u, SUM(CASE WHEN IsRefresh = 0 THEN 1 ELSE 0 END) AS not_refresh FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
