SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE (SearchPhrase <> '') AND IsRefresh = 0 GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
