import os
import sys
import yaml


# Manage Docker containers with the following CLAs (command line arguments)
#  -create -> Create containers
#  -remove -> Remove containers

# If os.system returns 0 from a command, that is considered success
SUCCESS = 0


def create(cmd, db):
    # Create container
    result = os.system(cmd)
    # Notify user
    if (result == SUCCESS):
        print(f'Success: created {db} container')
        return
    
    print(f'Error: Could not create {db} container')


def remove(container):
    # Stop container
    cmd = f'docker stop {container}'
    result_stop = os.system(cmd)

    if (result_stop == SUCCESS):
        # Remove container
        cmd = f'docker rm {container}'
        result_rm = os.system(cmd)
        # Notify user
        if result_rm == SUCCESS:
            print(f'Success: removed {container} container')
            return

    print(f'Error: Could not remove {container} container')


# Get CLAs
if len(sys.argv) > 1:
    # Prevent other scripts with CLAs from being called
    filename = os.path.basename(sys.argv[0])
    if filename == 'manage_containers.py':

        # Call relevant functions based on CLA
        argument = sys.argv[1]
        if argument == "-create":
            # Use YAML to separate the MYSQL password and easily change ports
            conn_mysql = yaml.safe_load(open('./dbs/conn_mysql.yaml'))
            conn_mongo = yaml.safe_load(open('./dbs/conn_mongo.yaml'))
            conn_redis = yaml.safe_load(open('./dbs/conn_redis.yaml'))
            conn_cass = yaml.safe_load(open('./dbs/conn_cass.yaml'))
            # Docker commands for creating db containers
            create(f'docker run -p {conn_mysql["port"]}:3306 --name some-mysql -e MYSQL_ROOT_PASSWORD={conn_mysql["password"]} -d mysql', 'mysql')
            create(f'docker run -p {conn_mongo["port"]}:27017 --name some-mongo -d mongo', 'mongo')
            create(f'docker run -p {conn_redis["port"]}:6379 --name some-redis -d redis', 'redis')
            create(f'docker run -p {conn_cass["port"]}:9042 --name some-cassandra -d cassandra', 'cassandra')
        if argument == "-remove":
            remove('some-mysql')
            remove('some-mongo')
            remove('some-redis')
            remove('some-cassandra')

