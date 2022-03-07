
import os
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
conn = yaml.safe_load(open('conn_mysql.yaml'))
cnx = pymysql.connect(user=conn['user'], 
    password=conn['password'],
    host=conn['host'],
    port=conn['port'])
    
# Create MYSQL cursor
cursor = cnx.cursor()


def init():
    # Create database
    cursor = cnx.cursor()
    query_db = 'CREATE DATABASE IF NOT EXISTS logs;'
    cursor.execute(query_db)
    cnx.commit()

    # Create table
    query_use = 'USE logs'
    cursor.execute(query_use)
    cnx.commit()
    query_tbl = 'CREATE TABLE log (id VARCHAR(36), stamp VARCHAR(20));'
    cursor.execute(query_tbl)
    cnx.commit()


def write():
    id = str(uuid.uuid4())
    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    query = f'INSERT INTO log VALUES ("{id}", "{time}")'
    cursor.execute(query)
    cnx.commit()

def read():
    query = "SELECT * FROM log ORDER BY stamp DESC LIMIT 5;"
    cursor.execute(query)

    stamps = {}

    for row in cursor.fetchall():
        stamps[row[0]] = row[1]

    return stamps

def empty():
    query = "TRUNCATE TABLE log"
    cursor.execute(query)  


@atexit.register
def exit_handler():
    cursor.close()
    cnx.close() 