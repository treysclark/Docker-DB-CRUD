import pymongo 
import yaml


# Stores the following functions for the Mongo database:
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


conn = yaml.safe_load(open('dbs/conn_mongo.yaml'))
client = pymongo.MongoClient(f'mongodb://{conn["host"]}:{conn["port"]}/{conn["db"]}')

db = client.logs
log = db.log


def write(stamps):
    for key, value in stamps.items():
        item = {
            "stamp" :value,
            "id":key 
        }

        # Duplicate Check: Prevent inserting previous timestamps
        filter = {key:value}
        new_value = {"$set": item}
        log.update_one(filter, new_value, upsert=True)


def read():
    stamps = {}
    # Show only the 5 most recent timestamps
    for log in log.find().sort('stamp', pymongo.DESCENDING).limit(5):
        stamps[log["id"]] = log["stamp"]
    return stamps


def empty():
    #Truncate logs collection
    db.logs.drop()
