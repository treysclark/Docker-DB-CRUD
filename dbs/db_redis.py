import redis
import yaml


# Stores the following functions for the Redis database:
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


conn = yaml.safe_load(open('dbs/conn_redis.yaml'))
client = redis.Redis(host=conn["host"], port=conn["port"], db=0)


def write(stamps):
    # Store multiple key/values and execute at one time
    pipe = client.pipeline()
    for key, value in stamps.items():
        pipe.set(key, value)
    pipe.execute()


def read():
    # Get all keys
    keys = client.keys()
    # Get all values in one db call
    pipe = client.pipeline()
    for key in keys:
        pipe.get(key)

    # Convert from redis standard byte format
    keys = [key.decode("utf-8") for key in keys]
    values = [value.decode("utf-8") for value in pipe.execute()]
    
    # Create dictionary for consistency between dbs
    unsorted_dict = dict(zip(keys, values))

    # Sort by timestamps before returning
    return dict(sorted(unsorted_dict.items(), key=lambda item: item[1], reverse=True))
        

def empty():
    client.flushdb()

