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
