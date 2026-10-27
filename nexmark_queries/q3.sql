-- Nexmark q3 (local item suggestion): sellers in OR/ID/CA listing category 10, newest auctions
SELECT P.name, P.city, P.state, A.id FROM auction AS A JOIN person AS P ON A.seller = P.id
WHERE A.category = 10 AND (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA') ORDER BY A.id DESC LIMIT 100;
