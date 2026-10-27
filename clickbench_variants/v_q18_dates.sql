SELECT UserID, SearchPhrase, COUNT(*) FROM hits WHERE EventDate >= DATE '2013-07-10' GROUP BY UserID, SearchPhrase LIMIT 10;
