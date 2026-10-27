SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY ROLLUP(MobilePhone, MobilePhoneModel) ORDER BY u DESC LIMIT 10;
