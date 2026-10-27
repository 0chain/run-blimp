SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE (MobilePhoneModel <> '') AND EventDate >= DATE '2013-07-10' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
