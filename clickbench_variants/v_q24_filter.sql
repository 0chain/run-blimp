SELECT * FROM hits WHERE (URL LIKE '%google%') AND IsRefresh = 0 ORDER BY EventTime LIMIT 10;
