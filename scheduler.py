from threading import Timer
import time

import dbs.db_mysql as db_mysql
import dbs.db_mongo as db_mongo
import dbs.db_redis as db_redis
import dbs.db_cass as db_cass


# The following reads and writes to and from the databases
# Then it notifies the user of the writes

        

def status(stamps, db):
    print(f'Data in {db}:')
    # Check if there is more than one key/value pair
    if isinstance(stamps, list):
        for stamp in stamps:
            for key, value in stamp.items():
                print(value)
    elif isinstance(stamps, dict):
        for key, value in stamps.items():
            print(value)
    time.sleep(1)


def mysql():
    db_mysql.write()


def mongo():
    stamps = db_mysql.read()
    status(stamps,'mysql')
    db_mongo.write(stamps)


def redis():
    stamps = db_mongo.read()
    status(stamps,'mongo')
    db_redis.write(stamps)


def cassandra():
    stamps = db_redis.read()
    status(stamps, 'redis')
    db_cass.write(stamps)
    stamps = db_cass.read()
    status(stamps, "cassandra")


def timeloop():    
    print(f'--- LOOP: ' + time.ctime() + ' ---')
    mysql()
    mongo()
    redis()
    cassandra()
    Timer(3, timeloop).start()

timeloop()