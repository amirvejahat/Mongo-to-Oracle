## Mongo To Oracle

this is a simple script to transmit data from mongo to oracle

## How to Run

1. install oracle oracle-clientinstant-basic
2. install redis server and make sure listen to default port 6379 
3. make sure you've an access to mongo database (shard/single instance) [mongo -h hostname -p port -d DB -c COLLECTION ]
4. change config file with your configuration hostname/username/password and etc.

[ you don't have to create listener.ora file to connect to oracle we'll handle in connections in Connections.py file ] 

## Schema.txt

this file is used for mapping mongo fields and oracle columns


## Evaluate.txt

in initial connection to oracle database evaluate.txt will generated automatically based on table structure.  

evaluate.txt file helps us to make sure data_type is correct before insert to the database

## Todo

- [x]  install oracle-clientinstant-basic package
- [x]  create connection class to connect to oracle instance
- [x]  create connection class to connect to mongo database
- [x]  create requirements.txt file for build package
- [x]  create config file
- [x]  run app in virtual environment 
- [ ]  handle connection_log_id[bson type] issue
- [ ]  add PS*/SQL returning CONNECTION_LOG_ID to make sure connection_logs inserted successfully
- [x]  performance tuning (Current 10 Pool,10K BatchSize,500K Records Data  80s)
- [x]  change the code to be compatible with multi threads
- [x]  insert corrupted cdr's connection_id to redis
- [ ]  add failover mongo connection 
- [x]  export data structure of oracle automaticlly
- [x]  fix schema.corrupted_cdr might get very large and fill up memory
