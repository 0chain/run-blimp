SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY ROLLUP(UserID, SearchPhrase) LIMIT 10;
