SELECT UserID, SearchPhrase, COUNT(*), COUNT(DISTINCT UserID) AS users FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
