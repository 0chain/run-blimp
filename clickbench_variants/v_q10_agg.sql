SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID), SUM(ResolutionWidth) AS sum_rw FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
