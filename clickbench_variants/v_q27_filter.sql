SELECT SearchPhrase FROM hits WHERE (SearchPhrase <> '') AND IsRefresh = 0 ORDER BY EventTime, SearchPhrase LIMIT 10;
