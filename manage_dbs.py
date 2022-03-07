
import os
import sys
import pymysql
import pymongo
import dbs.db_mysql as db_mysql
import dbs.db_mongo as db_mongo
import dbs.db_redis as db_redis
import dbs.db_cass as db_cass

# Manage databases with the following CLAs (command line arguments)
#  -init   -> Initialize MYSQL and Cassandra databases
#  -empty  -> Truncate databases


def init():
    db_mysql.init()
    db_cass.init()

def empty():
    db_mysql.init()
    db_mongo.init()
    db_redis.init()
    db_cass.init()


# Get CLAs
if len(sys.argv) > 1:
    # Prevent other scripts with CLAs from being called
    filename = os.path.basename(sys.argv[0])
    if filename == 'container.py':
        # Call relevant functions
        argument = sys.argv[1]
        if argument == "-init":
            init()
        if argument == "-empty":
            empty()