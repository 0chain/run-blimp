SELECT UserID, extract(minute FROM to_timestamp(EventTime)) AS m, COUNT(*) FROM hits GROUP BY UserID, m ORDER BY COUNT(*) DESC LIMIT 10;
