-- +goose Up
-- +goose StatementBegin
CREATE MATERIALIZED VIEW product2
AS
SELECT
    pc.productcategoryid AS pcid,
    p.productid AS productid,
    pc.name AS pcname,
    p.name AS pname
FROM product p JOIN productsubcategory ps ON p.productsubcategoryid = ps.productsubcategoryid
               JOIN productcategory pc ON ps.productcategoryid = pc.productcategoryid;

CREATE MATERIALIZED VIEW country2
AS
SELECT DISTINCT a.countryregioncode AS countrycode
FROM customer c JOIN customeraddress ca ON c.customerid = ca.customerid
                JOIN address a ON ca.addressid = a.addressid
WHERE ca.addresstype = 'Main Office';

GRANT SELECT ON product2 TO planadmin;
GRANT SELECT ON product2 TO planmanager;

GRANT SELECT ON country2 TO planadmin;
GRANT SELECT ON country2 TO planmanager;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE SELECT ON country2 FROM planmanager;
REVOKE SELECT ON country2 FROM planadmin;

REVOKE SELECT ON product2 FROM planmanager;
REVOKE SELECT ON product2 FROM planadmin;

DROP MATERIALIZED VIEW country2;

DROP MATERIALIZED VIEW product2;
-- +goose StatementEnd