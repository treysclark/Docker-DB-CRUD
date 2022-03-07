
import os
import sys
import pymysql
import pymongo

# Manage databases with the following CLAs (command line arguments)
#  -init   -> Initialize MYSQL and Cassandra databases
#  -empty  -> Truncate databases


def init():
    pass

def empty():
    pass


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