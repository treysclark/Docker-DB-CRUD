import sys 
import redis
import yaml


# Stores the following functions for the Redis database:
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


# Create connection to Redis database
try:
    conn = yaml.safe_load(open('dbs/conn_redis.yaml'))
    client = redis.Redis(host=conn["host"], port=conn["port"], db=0)
except Exception as e:
    print("Error: redis connection\n", e)
    sys.exit()


def write(stamps):
    # Store multiple key/values and execute at one time
    pipe = client.pipeline()
    for key, value in stamps.items():
        pipe.set(key, value)

    try:
        pipe.execute()
    except Exception as e:
        print('Error: redis did not store key/values\n', e)
        sys.exit()


def read():
    try:
        # Get all keys
        keys = client.keys()
    except Exception as e:
        print('Error: redis did not retrieve keys\n', e)
        sys.exit()

    # Get all values in one db call
    pipe = client.pipeline()
    for key in keys:
        pipe.get(key)

    # Convert from redis standard byte format
    keys = [key.decode("utf-8") for key in keys]

    try:
        values = [value.decode("utf-8") for value in pipe.execute()]
    except Exception as e:
        print('Error: redis did not retrieve values\n', e)
        sys.exit()

    # Create dictionary for consistency between dbs
    unsorted_dict = dict(zip(keys, values))

    # Sort by timestamps before returning
    # Source: https://stackoverflow.com/a/613218/848353
    sorted_dict = dict(sorted(unsorted_dict.items(), key=lambda item: item[1], reverse=True))

    # Limit results to the most recent three timestamps
    # Source: https://stackoverflow.com/a/12980510/848353
    return  {k: sorted_dict[k] for k in list(sorted_dict)[:3]}
        

def empty():
    try:
        client.flushdb()
    except Exception as e:
        print('Error: redis was not emptied\n', e)
        return

    print("Success: redis emptied")

