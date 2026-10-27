SELECT AdvEngineID, COUNT(*), SUM(ResolutionWidth) AS sum_rw FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
