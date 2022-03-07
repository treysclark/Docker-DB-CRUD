#pip install cassandra-driver
from cassandra.cluster import Cluster
from datetime import datetime
import yaml

# Stores the following functions for the Cassandra database:
#   init():  Initialize Cassandra database
#   write(): Insert the current timestamp into the db
#   read():  Select the last 5 timestamps
#   empty(): Truncate the table
# The 'manage_dbs.py' script will access these functions


conn = yaml.safe_load(open('dbs/conn_cass.yaml'))
cluster = Cluster([conn["host"]], port=conn["port"])

# Bucket: used for ordering by stamps to make it consistent with the other dbs print stmts
# Source: https://stackoverflow.com/a/34495325/848353
day = datetime.today().strftime('%Y-%m-%d')


def init():
    # Keyspace has not been created, so reset var to None
    keyspace = None  
    session = cluster.connect(keyspace)

    # Create keyspace
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS logs
                    WITH REPLICATION = {'class':'SimpleStrategy',
                        'replication_factor':1}
                    """)
    session.set_keyspace('stamps')
    # Create table
    # Make sure cluster is based on stamp to keep ordering the same as the other dbs
    session.execute("""
                    CREATE TABLE IF NOT EXISTS log (
                        day text,
                        id text,
                        stamp text,
                        PRIMARY KEY (day, stamp)
                    )  WITH CLUSTERING ORDER BY (stamp DESC);
                    """)
    

def write(stamps):

    keyspace = 'logs'
    session = cluster.connect(keyspace)

    for stamp in stamps.items():
        sql = (f"INSERT INTO log (day, id, stamp) VALUES ('{day}', '{stamp[0]}','{stamp[1]}')")
        session.execute(sql)


def read():

    keyspace = 'logs'
    session = cluster.connect(keyspace)

    results = session.execute(f"SELECT * FROM log WHERE day = '{day}' ORDER BY stamp DESC")
    dict_results = {}
    for result in results:
        dict_results[result.id] = result.stamp

    return dict_results


def empty():

    keyspace = 'logs'
    session = cluster.connect(keyspace)

    session.execute("TRUNCATE TABLE log")
