SELECT MobilePhone, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone ORDER BY u DESC LIMIT 10;
