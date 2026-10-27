SELECT MIN(EventDate), MAX(EventDate), COUNT(DISTINCT UserID) AS users FROM hits;
