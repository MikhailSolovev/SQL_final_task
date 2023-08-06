-- +goose Up
-- +goose StatementBegin
INSERT INTO company_sales(cid, salesamt, year, quarter_yr, qr, categoryid, ccls)
SELECT
    co.id AS cid,
    SUM(sod.linetotal) AS salesamt,
    DATE_PART('year', sh.orderdate) AS year,
    DATE_PART('quarter', sh.orderdate) AS quarter_yr,
    DATE_PART('year', sh.orderdate) || '.' || DATE_PART('quarter', sh.orderdate) AS qr,
    p.pcid AS categoryid,
    coa.cls AS ccls
FROM salesorderdetail sod JOIN salesorderheader sh ON sod.salesorderid = sh.salesorderid
                          JOIN customer cust ON sh.customerid = cust.customerid
                          JOIN product2 p on sod.productid = p.productid
                          JOIN company co ON cust.companyname = co.cname
                          JOIN company_abc coa ON co.id = coa.cid AND
                                                  coa.year = DATE_PART('year', sh.orderdate)
WHERE DATE_PART('year', sh.orderdate) IN ('2012', '2013')
GROUP BY co.id, DATE_PART('year', sh.orderdate), DATE_PART('quarter', sh.orderdate),
         p.pcid, coa.cls;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
TRUNCATE TABLE company_sales;
-- +goose StatementEnd
