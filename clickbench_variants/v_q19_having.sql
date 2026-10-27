SELECT UserID, extract(minute FROM to_timestamp(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase HAVING COUNT(*) > 10 ORDER BY COUNT(*) DESC LIMIT 10;
