SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE (SearchPhrase <> '') AND EventDate >= DATE '2013-07-10' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
