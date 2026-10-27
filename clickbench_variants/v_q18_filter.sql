SELECT UserID, SearchPhrase, COUNT(*) FROM hits WHERE IsRefresh = 0 GROUP BY UserID, SearchPhrase LIMIT 10;
