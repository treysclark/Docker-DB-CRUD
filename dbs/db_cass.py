import sys
from cassandra.cluster import Cluster
from datetime import datetime
import yaml

# Stores the following functions for the Cassandra database:
#   init():  Initialize Cassandra database
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


# Create connection to Cassandra database
try:
    conn = yaml.safe_load(open('dbs/conn_cass.yaml'))
    cluster = Cluster([conn["host"]], port=conn["port"])
    # Keyspace has not been created, so reset var to None
    keyspace = None  
    session = cluster.connect(keyspace)
except Exception as e:
    print("Error: cassandra connection\n", e)
    sys.exit()

# Bucket: used for ordering by stamps to make it consistent with the other dbs print stmts
# Source: https://stackoverflow.com/a/34495325/848353
day = datetime.today().strftime('%Y-%m-%d')


def init():
    try:
        # Create keyspace
        session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS docker_db
                        WITH REPLICATION = {'class':'SimpleStrategy',
                            'replication_factor':1}
                        """)
    except Exception as e:
        print("Error: cassandra keyspace creation\n", e)
        return

    set_keyspace()

    # Create table
    # Make sure cluster is based on stamp to keep ordering the same as the other dbs
    try:
        session.execute("""
                        CREATE TABLE IF NOT EXISTS log (
                            day text,
                            id text,
                            stamp text,
                            PRIMARY KEY (day, stamp)
                        )  WITH CLUSTERING ORDER BY (stamp DESC);
                        """)
    except Exception as e:
        print("Error: cassandra table creation\n", e)
        return

    print("Success: cassandra initialized")


def write(stamps):
    set_keyspace()

    for stamp in stamps.items():
        sql = (f"INSERT INTO log (day, id, stamp) VALUES ('{day}', '{stamp[0]}','{stamp[1]}')")
        try:
            session.execute(sql)
        except Exception as e:
            print(f'Error: cassandra writing log table\n', e)


def read():
    set_keyspace()
    
    try: 
        # Select only the 3 most recent timestamps
        results = session.execute(f"SELECT * FROM log WHERE day = '{day}' ORDER BY stamp DESC LIMIT 3")
    except Exception as e:
        print('Error: cassandra reading log table\n', e)

    dict_results = {}
    for result in results:
        dict_results[result.id] = result.stamp

    return dict_results


def empty():
    set_keyspace()

    try: 
        session.execute("TRUNCATE TABLE log")
    except Exception as e:
        print('Error: cassandra emptying log table\n', e)
        return
    
    print("Success: cassandra log table emptied")


def set_keyspace():
    try:
        session.set_keyspace(f'{conn["db"]}')
    except Exception as e:
        print("Error: cassandra setting keyspace\n", e)