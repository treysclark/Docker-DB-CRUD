import sys
import pymongo 
import yaml


# Stores the following functions for the Mongo database:
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


# Create connection to Mongo database
try:
    conn = yaml.safe_load(open('dbs/conn_mongo.yaml'))
    client = pymongo.MongoClient(f'mongodb://{conn["host"]}:{conn["port"]}/{conn["db"]}')
    
    db = client[conn["db"]]
    log = db.log
except Exception as e:
    print("Error: mongo connection\n", e)
    sys.exit()



def write(stamps):
    for key, value in stamps.items():
        item = {
            "stamp" :value,
            "id":key 
        }

        # Duplicate Check: Prevent inserting previous timestamps
        filter = {key:value}
        new_value = {"$set": item}

        try:
            log.update_one(filter, new_value, upsert=True)
        except Exception as e:
            print('Error: mongo did not insert document\n', e)
            sys.exit()

def read():
    stamps = {}

    try:
        # Show only the 3 most recent timestamps
        for row in log.find().sort('stamp', pymongo.DESCENDING).limit(3):
            stamps[row["id"]] = row["stamp"]
    except Exception as e:
        print('Error: mongo did not retrieve documents\n', e)
        sys.exit()

    return stamps


def empty():
    try:
        # Truncate logs collection
        db.log.drop()
    except Exception as e:
        print('Error: mongo did not empty collection\n', e)

    print("Success: mongo log collection was emptied")
