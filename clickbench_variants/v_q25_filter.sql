SELECT SearchPhrase FROM hits WHERE (SearchPhrase <> '') AND IsRefresh = 0 ORDER BY EventTime LIMIT 10;
