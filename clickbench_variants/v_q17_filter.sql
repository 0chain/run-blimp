SELECT UserID, SearchPhrase, COUNT(*) FROM hits WHERE IsRefresh = 0 GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
