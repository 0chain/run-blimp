SELECT UserID, SearchPhrase, COUNT(*), SUM(ResolutionWidth) AS sum_rw FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
