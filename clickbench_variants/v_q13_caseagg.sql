SELECT SearchPhrase, COUNT(*) AS c, SUM(CASE WHEN IsRefresh = 0 THEN 1 ELSE 0 END) AS not_refresh FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
