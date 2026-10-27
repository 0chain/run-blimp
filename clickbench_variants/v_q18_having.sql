SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase HAVING COUNT(*) > 10 LIMIT 10;
