-- Nexmark q8 (monitor new users): people who opened an auction in the 10 s window they joined
SELECT P.id, P.name, P.starttime FROM (
  SELECT id, name, time_bucket(INTERVAL 10 SECOND, dateTime) AS starttime FROM person GROUP BY id, name, starttime) P
JOIN (SELECT seller, time_bucket(INTERVAL 10 SECOND, dateTime) AS starttime FROM auction GROUP BY seller, starttime) A
ON P.id = A.seller AND P.starttime = A.starttime ORDER BY P.starttime DESC, P.id LIMIT 100;
