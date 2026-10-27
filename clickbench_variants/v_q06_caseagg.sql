SELECT COUNT(DISTINCT SearchPhrase), SUM(CASE WHEN IsRefresh = 0 THEN 1 ELSE 0 END) AS not_refresh FROM hits;
