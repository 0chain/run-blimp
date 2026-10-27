SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u, SUM(ResolutionWidth) AS sum_rw FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
