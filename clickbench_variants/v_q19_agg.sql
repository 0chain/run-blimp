SELECT UserID, extract(minute FROM to_timestamp(EventTime)) AS m, SearchPhrase, COUNT(*), SUM(ResolutionWidth) AS sum_rw FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
