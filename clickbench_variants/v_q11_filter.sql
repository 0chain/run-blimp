SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE (MobilePhoneModel <> '') AND IsRefresh = 0 GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
