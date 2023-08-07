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
