SELECT AdvEngineID, COUNT(*), SUM(CASE WHEN IsRefresh = 0 THEN 1 ELSE 0 END) AS not_refresh FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
