SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits WHERE IsRefresh = 0 GROUP BY RegionID ORDER BY c DESC LIMIT 10;
