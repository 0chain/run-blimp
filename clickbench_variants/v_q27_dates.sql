SELECT SearchPhrase FROM hits WHERE (SearchPhrase <> '') AND EventDate >= DATE '2013-07-10' ORDER BY EventTime, SearchPhrase LIMIT 10;
