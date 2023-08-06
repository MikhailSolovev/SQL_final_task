-- +goose Up
-- +goose StatementBegin
INSERT INTO company_abc(cid, salestotal, cls, year)
WITH prj AS (
    SELECT
        co.id,
        co.cname,
        SUM(sh.subtotal) AS salestotal,
        DATE_PART('year', sh.orderdate) AS year
    FROM salesorderheader sh JOIN customer c ON sh.customerid = c.customerid
                             JOIN company co ON c.companyname = co.cname
    WHERE DATE_PART('year', sh.orderdate) IN ('2013', '2012')
    GROUP BY co.id, DATE_PART('year', sh.orderdate)
), sbt AS (SELECT *,
                  SUM(salestotal) OVER (PARTITION BY year ORDER BY salestotal DESC) AS runsubtotal
           FROM prj
), bnd AS (
    SELECT
        year,
        SUM(salestotal) * 0.8 AS sa,
        SUM(salestotal) * 0.95 AS sb
    FROM prj
    GROUP BY year
)
SELECT
    id AS cid,
    salestotal,
    CASE
        WHEN runsubtotal <= sa THEN 'A'
        WHEN runsubtotal <= sb THEN 'B'
        ELSE 'C'
        END AS cls,
    sbt.year
FROM sbt JOIN bnd ON sbt.year = bnd.year;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
TRUNCATE TABLE company_abc;
-- +goose StatementEnd
