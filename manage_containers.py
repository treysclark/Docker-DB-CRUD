import os
import sys
import yaml

# Manage Docker containers with the following CLAs (command line arguments)
#  -create -> Create containers
#  -init   -> Initialize MYSQL and Cassandra databases
#  -stop   -> Stop containers
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
        # Call relevant functions
        argument = sys.argv[1]
        if argument == "-create":
            conn = yaml.safe_load(open('./dbs/conn_mysql.yaml'))
            # Docker commands for creating db containers
            create(f'docker run -p {conn["port"]}:3306 --name some-mysql -e MYSQL_ROOT_PASSWORD={conn["password"]} -d mysql', 'mysql')
            create('docker run -p 27017:27017 --name some-mongo -d mongo', 'mongo')
            create('docker run -p 6379:6379 --name some-redis -d redis', 'redis')
            create('docker run -p 9042:9042 --name some-cassandra -d cassandra', 'cassandra')
    
        if argument == "-remove":
            remove('some-mysql')
            remove('some-mongo')
            remove('some-redis')
            remove('some-cassandra')

