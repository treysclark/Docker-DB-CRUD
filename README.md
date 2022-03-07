# ReadWriteDockerDBs

Create Docker containers for popular databases (MYSQL, MongoDB, Redis, and Cassandra). Then automate reading and writing data to each database using Python time loops.

## Requirements:
The following drivers are required (`pip install <driver-name>`): 
- cassandra-driver 
- pyaml 
- pymongo
- pymysql
- redis 

## Manage Containers:
Container management consists of creating and removing containers from the command line.

- **Create Containers**: `python manage_containers.py -create`

![Creating containers](/imgs/manage_containers_create.gif)

&nbsp;

- **Remove Containers**: `python manage_containers.py -remove`

![Removing containers](/imgs/manage_containers_remove.gif)

&nbsp;
&nbsp;

### Manage Databases:
Database management consists of initializing (MYSQL and CassandraDB) and emptying the databases.

- **Initialize Databases**: `python manage_dbs.py -init`

![Initialize databases](/imgs/manage_dbs_init.gif)

&nbsp;

- **Empty Databases**: `python manage_dbs.py -empty`

![Empty databases](/imgs/manage_dbs_empty.gif)

&nbsp;
&nbsp;

### Scheduler:
The scheduler uses a Python threading Timer to make a recursive call to the timeloop function. The timeloop function then calls the the read and write functions of the databases. The script also incorporates two sleep functions for a total of a 4 second delay. 

Run the script with the following command: `python scheduler.py` 

![Scheduler](/imgs/scheduler.gif)
