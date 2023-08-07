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

```python
import psycopg2
from decouple import Config, RepositoryEnv

delete_plan_data = '''
DELETE
FROM plan_data
WHERE quarterid = %(quarterID)s
'''

delete_plan_status = '''
DELETE
FROM plan_status
WHERE quarterid = %(quarterID)s
'''

create_plan_status_records = '''
INSERT INTO plan_status(quarterid, status, modifieddatetime, author, country) 
SELECT DISTINCT
    %(quarterID)s  AS quarterid,
    'R' AS status,
    NOW() AS modifieddatetime,
    %(author)s AS author,
    countrycode AS country
FROM company
'''

generate_plan_data = '''
WITH cte AS (
    SELECT
        'N' AS versionid,
        c.countrycode AS country,
        cs.year,
        cs.quarter_yr,
        cs.categoryid AS pcid,
        SUM(cs.salesamt) AS salesamt
    FROM company_sales cs JOIN company c ON  cs.cid  = c.id
    WHERE cs.ccls IN ('A', 'B') AND cs.year IN (%(year)s - 1, %(year)s  - 2) AND cs.quarter_yr = %(quarter)s 
    GROUP BY(cs.year, cs.quarter_yr, c.countrycode, cs.categoryid)
    ), cte1 AS (
    SELECT DISTINCT
        c1.versionid,
        c1.country,
        %(year)s || '.' || %(quarter)s AS quarterid,
        c1.pcid,
        CASE
            WHEN c1.salesamt IS NULL THEN c2.salesamt
            WHEN c2.salesamt IS NULL THEN c1.salesamt
            ELSE (c1.salesamt + c2.salesamt) / 2
        END AS salesamt
    FROM cte c1 FULL JOIN cte c2 ON c1.country = c2.country AND c1.pcid = c2.pcid
                                    AND c1.quarter_yr = c2.quarter_yr
    WHERE c1.year != c2.year
    ), cte2 AS (
    SELECT DISTINCT c.countrycode AS country, cs.categoryid AS pcid 
    FROM company_sales cs JOIN company c ON cs.cid  =  c.id
    EXCEPT 
    SELECT DISTINCT country, pcid 
    FROM cte1
    )
INSERT INTO plan_data(versionid, country, quarterid, pcid, salesamt) 
SELECT *
FROM cte1
UNION 
SELECT
    'N' AS versionid,
    cte2.country,
    %(year)s || '.' || %(quarter)s AS quarterid,
    cte2.pcid,
    0 AS salesamt
FROM cte2
'''

copy_plan_data = '''
INSERT INTO plan_data(versionid, country, quarterid, pcid, salesamt)
SELECT
    'P' AS versionid,
    country,
    quarterid,
    pcid,
    salesamt
FROM plan_data
'''


def start_planning(year, quarter, user, pwd):
    env = Config(RepositoryEnv('.env'))
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database=env.get('POSTGRES_DB'),
        user=user,
        password=pwd
    )

    cursor = connection.cursor()
    connection.autocommit = False

    try:
        cursor.execute(delete_plan_data, {"quarterID": f"{year}.{quarter}"})
        cursor.execute(delete_plan_status, {"quarterID": f"{year}.{quarter}"})
        cursor.execute(create_plan_status_records, {"quarterID": f"{year}.{quarter}", "author": user})
        cursor.execute(generate_plan_data, {"year": year, "quarter": quarter})
        cursor.execute(copy_plan_data)
        connection.commit()
    except Exception as err:
        print("Error:", err)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


start_planning(2014, 1, 'ivan', 'password')
```

**plan_data, first 20:**
```csv
N,DE,2014.1,4,0.00
N,DE,2014.1,2,0.00
P,FR,2014.1,1,0.00
N,GB,2014.1,4,0.00
P,FR,2014.1,2,0.00
P,DE,2014.1,2,0.00
N,FR,2014.1,4,0.00
N,FR,2014.1,3,0.00
P,FR,2014.1,3,0.00
N,FR,2014.1,2,0.00
P,DE,2014.1,4,0.00
N,FR,2014.1,1,0.00
P,FR,2014.1,4,0.00
P,GB,2014.1,4,0.00
N,GB,2014.1,3,168.87
P,GB,2014.1,3,168.87
N,AU,2014.1,4,753.67
P,AU,2014.1,4,753.67
N,DE,2014.1,3,1246.06
P,DE,2014.1,3,1246.06
```

**plan_status:**
```csv
2014.1,R,2023-08-06 19:56:24.151153,ivan,AU
2014.1,R,2023-08-06 19:56:24.151153,ivan,GB
2014.1,R,2023-08-06 19:56:24.151153,ivan,US
2014.1,R,2023-08-06 19:56:24.151153,ivan,CA
2014.1,R,2023-08-06 19:56:24.151153,ivan,DE
2014.1,R,2023-08-06 19:56:24.151153,ivan,FR
```

### Task #7. Changing plan data

```python
import psycopg2
from decouple import Config, RepositoryEnv

set_lock_query = '''
UPDATE plan_status
SET status = 'L',
    modifieddatetime = NOW(),
    author = %(author)s
WHERE quarterid = %(quarterID)s AND
      country IN (SELECT country FROM country_managers WHERE username = %(author)s)
'''

remove_lock_query = '''
UPDATE plan_status
SET status = 'R',
    modifieddatetime = NOW(),
    author = %(author)s
WHERE quarterid = %(quarterID)s AND
      country IN (SELECT country FROM country_managers WHERE username = %(author)s)
'''

increase_volume_query = '''
UPDATE v_plan_edit
SET salesamt = salesamt * (1 + floor(random() * (%(h)s - %(l)s + 1) + %(l)s) / 100)
'''


def set_lock(year, quarter, user, pwd):
    env = Config(RepositoryEnv('.env'))
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database=env.get('POSTGRES_DB'),
        user=user,
        password=pwd
    )

    cursor = connection.cursor()
    connection.autocommit = False

    try:
        cursor.execute(set_lock_query, {"quarterID": f"{year}.{quarter}", "author": user})
        connection.commit()
    except Exception as err:
        print("Error:", err)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def remove_lock(year, quarter, user, pwd):
    env = Config(RepositoryEnv('.env'))
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database=env.get('POSTGRES_DB'),
        user=user,
        password=pwd
    )

    cursor = connection.cursor()
    connection.autocommit = False

    try:
        cursor.execute(remove_lock_query, {"quarterID": f"{year}.{quarter}", "author": user})
        connection.commit()
    except Exception as err:
        print("Error:", err)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def increase_volume(user, pwd):
    env = Config(RepositoryEnv('.env'))
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database=env.get('POSTGRES_DB'),
        user=user,
        password=pwd
    )

    cursor = connection.cursor()
    connection.autocommit = False

    try:
        cursor.execute(increase_volume_query, {"h": 50, "l": 30})
        connection.commit()
    except Exception as err:
        print("Error:", err)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


set_lock(2014, 1, 'kirill', 'password')
set_lock(2014, 1, 'sophie', 'password')
increase_volume('kirill', 'password')
increase_volume('sophie', 'password')
remove_lock(2014, 1, 'kirill', 'password')
remove_lock(2014, 1, 'sophie', 'password')
```

```csv
AU,2014.1,1,130620.49,P
AU,2014.1,2,14405.04,P
AU,2014.1,3,2960.40,P
AU,2014.1,4,753.67,P
DE,2014.1,1,36045.17,P
DE,2014.1,2,0.00,P
DE,2014.1,3,1246.06,P
DE,2014.1,4,0.00,P
FR,2014.1,1,0.00,P
FR,2014.1,2,0.00,P
FR,2014.1,3,0.00,P
FR, 2014.1,4,0.00,P
GB,2014.1,1,66549.83,P
GB,2014.1,2,3898.64,P
GB,2014.1,3,168.87,P
GB,2014.1,4,0.00,P
```

### Task #8. Plan data approval

```python
import psycopg2
from decouple import Config, RepositoryEnv

accept_status_query = '''
UPDATE plan_status
SET status = 'A',
    modifieddatetime = NOW(),
    author = %(author)s
WHERE quarterid = %(quarterID)s AND
      status = 'R' AND
      country IN (SELECT country FROM country_managers WHERE username = %(author)s)
'''

clear_query = '''
DELETE
FROM plan_data
WHERE quarterid = %(quarterID)s AND
      versionid = 'A' AND
      country IN (SELECT country FROM country_managers WHERE username = %(author)s)
'''

copy_to_accept_query = '''
INSERT INTO plan_data(versionid, country, quarterid, pcid, salesamt)
SELECT
    'A' AS versionid,
    country,
    quarterid,
    pcid,
    salesamt
FROM plan_data
WHERE quarterid = %(quarterID)s AND
      versionid = 'P' AND
      country IN (SELECT country FROM country_managers WHERE username = %(author)s)
'''


def accept_plan(year, quarter, user, pwd):
    env = Config(RepositoryEnv('.env'))
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database=env.get('POSTGRES_DB'),
        user=user,
        password=pwd
    )

    cursor = connection.cursor()
    connection.autocommit = False

    try:
        cursor.execute(clear_query, {"quarterID": f"{year}.{quarter}", "author": user})
        cursor.execute(copy_to_accept_query, {"quarterID": f"{year}.{quarter}", "author": user})
        cursor.execute(accept_status_query, {"quarterID": f"{year}.{quarter}", "author": user})
        connection.commit()
    except Exception as err:
        print("Error:", err)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


accept_plan(2014, 1, 'kirill', 'password')
accept_plan(2014, 1, 'sophie', 'password')
```

**kirill v_plan:**
```csv
AU,1,2014.1,171112.84
AU,2,2014.1,20743.26
AU,3,2014.1,3878.12
AU,4,2014.1,1077.75
DE,1,2014.1,52986.40
DE,2,2014.1,0.00
DE,3,2014.1,1781.87
DE,4,2014.1,0.00
FR,1,2014.1,0.00
FR,2,2014.1,0.00
FR,3,2014.1,0.00
FR,4,2014.1,0.00
GB,1,2014.1,97828.25
GB,2,2014.1,5847.96
GB,3,2014.1,241.48
GB,4,2014.1,0.00
```

**sophie v_plan:**
```csv
CA,1,2014.1,675664.24
CA,2,2014.1,76858.60
CA,3,2014.1,7483.32
CA,4,2014.1,1731.76
US,1,2014.1,1430213.81
US,2,2014.1,197750.22
US,3,2014.1,22755.94
US,4,2014.1,5933.51
```

### Task #9. Data preparation for plan-fact analysis in Q1 2014

I used 'Calculate actual data using salesorderheader and ordersalesdetail tables without using
company_sales'.

```sql
CREATE MATERIALIZED VIEW mv_plan_fact_2014
AS
WITH fact AS (
    SELECT
        co.countrycode AS country,
        DATE_PART('year', sod.modifieddate) || '.' || DATE_PART('quarter', sod.modifieddate) AS qurter_id,
        p.pcid AS pcid,
        SUM(sod.linetotal) AS salesamt
    FROM salesorderdetail sod JOIN salesorderheader sh ON sod.salesorderid = sh.salesorderid
                              JOIN customer cust ON sh.customerid = cust.customerid
                              JOIN product2 p on sod.productid = p.productid
                              JOIN company co ON cust.companyname = co.cname
    WHERE DATE_PART('year', sod.modifieddate) = '2014' AND
            co.id IN (SELECT cid FROM company_sales WHERE ccls IN ('A', 'B') AND year = 2013)
    GROUP BY  co.countrycode, DATE_PART('year', sod.modifieddate), DATE_PART('quarter', sod.modifieddate), p.pcid)
SELECT
    fact.qurter_id AS "Quarter",
    fact.country AS "Country",
    fact.pcid AS "Category Name",
    CASE
        WHEN plan_data.salesamt IS NOT NULL THEN ROUND(plan_data.salesamt - fact.salesamt, 2)
        END AS "Dev.",
    CASE
        WHEN plan_data.salesamt IS NOT NULL AND plan_data.salesamt != 0 THEN
            ROUND((plan_data.salesamt - fact.salesamt) / plan_data.salesamt * 100, 2)
        END AS "Dev., %"
FROM fact LEFT JOIN plan_data ON plan_data.pcid = fact.pcid AND plan_data.quarterid = fact.qurter_id AND
                                 plan_data.country = fact.country AND plan_data.versionid = 'A';
```

```csv
2014.1,AU,1,-55841.46,-32.63
2014.1,AU,2,-3871.7,-18.66
2014.1,AU,3,-2184.85,-56.34
2014.1,AU,4,-2588.85,-240.21
2014.1,CA,1,416202.73,61.6
2014.1,CA,2,35521.47,46.22
2014.1,CA,3,-3075.7,-41.1
2014.1,CA,4,-1858.29,-107.31
2014.1,DE,1,-18584.59,-35.07
2014.1,DE,2,-8554.24,
2014.1,DE,3,-1063.8,-59.7
2014.1,DE,4,-1504.34,
2014.1,FR,1,-52040.77,
2014.1,FR,2,-8441.24,
2014.1,FR,3,-1147.97,
2014.1,US,1,-14314.01,-1
2014.1,US,2,-97029.62,-49.07
2014.1,US,3,-18321.49,-80.51
2014.1,US,4,-11858.44,-199.86
```