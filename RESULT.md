### Task #1. Access settings
```sql
INSERT INTO country_managers(username, country)
VALUES ('sophie', 'US'),
       ('sophie', 'CA'),
       ('kirill', 'FR'),
       ('kirill', 'GB'),
       ('kirill', 'DE'),
       ('kirill', 'AU');
```

### Task #2. product2 & country2 materialized views
```sql
CREATE MATERIALIZED VIEW product2
AS
SELECT
    pc.productcategoryid AS pcid,
    p.productid AS productid,
    pc.name AS pcname,
    p.name AS pname
FROM product p JOIN productsubcategory ps ON p.productsubcategoryid = ps.productsubcategoryid
               JOIN productcategory pc ON ps.productcategoryid = pc.productcategoryid;
```

```sql
CREATE MATERIALIZED VIEW country2
AS
SELECT DISTINCT a.countryregioncode AS countrycode
FROM customer c JOIN customeraddress ca ON c.customerid = ca.customerid
                JOIN address a ON ca.addressid = a.addressid
WHERE ca.addresstype = 'Main Office';
```

```sql
GRANT SELECT ON product2 TO planadmin;
GRANT SELECT ON product2 TO planmanager;

GRANT SELECT ON country2 TO planadmin;
GRANT SELECT ON country2 TO planmanager;
```

### Task #3. Loading data into the company table

```sql
INSERT INTO company(cname, countrycode, city)
SELECT DISTINCT c.companyname AS cname, a.countryregioncode AS countrycode, a.city AS city
FROM customer c JOIN customeraddress ca ON c.customerid = ca.customerid
                JOIN address a ON ca.addressid = a.addressid
WHERE ca.addresstype = 'Main Office';
```

### Task #4. Company classification

```sql
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
```

```csv
390,375493.4641,A,2012
350,351188.4604,A,2012
402,316681.8038,A,2012
282,301678.2118,A,2012
269,296800.7702,A,2012
313,289303.2579,A,2012
366,274221.0413,A,2012
288,265936.5862,A,2012
247,263035.9455,A,2012
348,219829.2882,A,2012
389,213869.4374,A,2012
351,202777.6034,A,2012
274,190732.7335,A,2012
297,186628.455,A,2012
312,174683.8141,A,2012
262,172701.4457,A,2012
365,166732.7648,A,2012
329,164883.5653,A,2012
337,154657.3032,A,2012
227,152685.4219,A,2012
```

### Task #5. Finding quarterly sales amount by company, and product category

```sql
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
```

### Task #6. Initial data preparation

