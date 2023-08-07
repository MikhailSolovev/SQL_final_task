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
