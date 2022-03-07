
import sys
import pymysql
import yaml
from datetime import datetime
import atexit
import uuid

# Stores the following functions for the MYSQL database:
#   init():  Initialize MYSQL database
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


# Create connection to MYSQL database
try:
    conn = yaml.safe_load(open('dbs/conn_mysql.yaml'))
    cnx = pymysql.connect(user=conn['user'], 
        password=conn['password'],
        host=conn['host'],
        port=conn['port'])

    # Create MYSQL cursor
    cursor = cnx.cursor()
except Exception as e:
    print("Error: mysql connection\n", e)
    sys.exit()
    

def init():
    try:
        # Create database
        cursor = cnx.cursor()
        query_db = 'CREATE DATABASE IF NOT EXISTS docker_db;'
        cursor.execute(query_db)
        cnx.commit()
    except Exception as e:
        print("Error: mysql database creation\n",e)
        return

    try:
        set_db()

        # Create table
        query_tbl = 'CREATE TABLE IF NOT EXISTS log (id VARCHAR(36), stamp VARCHAR(20));'
        cursor.execute(query_tbl)
        cnx.commit()
    except Exception as e:
        print("Error: mysql table creation\n",e)
        return

    # Notify user
    print("Success: mysql initialized")


def write():
    set_db()

    for row in cursor.fetchall():
        print(row)

    # Insert timestamps
    id = str(uuid.uuid4())
    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    query = f'INSERT INTO log VALUES ("{id}", "{time}")'
    cursor.execute(query)
    cnx.commit()


def read():
    set_db()

    # Select only the 3 most recent timestamps
    query = "SELECT * FROM log ORDER BY stamp DESC LIMIT 3;"
    cursor.execute(query)

    stamps = {}

    for row in cursor.fetchall():
        stamps[row[0]] = row[1]

    return stamps


def empty():
    set_db()

    try: 
        query = "TRUNCATE TABLE log"
        cursor.execute(query)  
    except Exception as e:
        print("Error: truncating mysql table\n", e)
        return
    
    print("Success: mysql log table emptied")


def set_db():
    # Choose docker_db database
    try: 
        query_use = 'USE docker_db'
        cursor.execute(query_use)
        cnx.commit()
    except Exception as e:
        print("Error: selecting mysql database\n", e)
        sys.exit()


@atexit.register
def exit_handler():
    # Close connections
    cursor.close()
    cnx.close() 