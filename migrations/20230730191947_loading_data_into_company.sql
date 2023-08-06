-- +goose Up
-- +goose StatementBegin
INSERT INTO company(cname, countrycode, city)
SELECT DISTINCT c.companyname AS cname, a.countryregioncode AS countrycode, a.city AS city
FROM customer c JOIN customeraddress ca ON c.customerid = ca.customerid
                JOIN address a ON ca.addressid = a.addressid
WHERE ca.addresstype = 'Main Office';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
TRUNCATE TABLE company;
-- +goose StatementEnd
