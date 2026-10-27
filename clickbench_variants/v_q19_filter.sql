SELECT UserID, extract(minute FROM to_timestamp(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits WHERE IsRefresh = 0 GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
