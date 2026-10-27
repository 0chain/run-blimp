SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel HAVING COUNT(*) > 10 ORDER BY u DESC LIMIT 10;
