SELECT UserID, extract(minute FROM to_timestamp(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY ROLLUP(UserID, m, SearchPhrase) ORDER BY COUNT(*) DESC LIMIT 10;
