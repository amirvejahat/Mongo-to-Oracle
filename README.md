## Todo

- [x]  install oracle-clientinstant-basic package
- [x]  create connection class to connect to oracle instance
- [x]  create connection class to connect to mongo database
- [x]  create requirements.txt file for build package
- [x]  create config file
- [x]  run app in virtual environment 
- [x]  handle connection_log_id[bson type] issue
- [x]  add PS*/SQL returning CONNECTION_LOG_ID to make sure connection_logs inserted successfully
- [x]  create a class to connect to postgresql to get connection sequential number
- [x]  performance tuning (Current 10 Pool,10K BatchSize,500K Records Data  80s)
- [x]  change the code to be compatible with multi threads
- [x]  insert corrupted cdr's connection_id to redis
- [x]  add failover mongo connection 
- [x]  export data structure of oracle automaticlly
- [x]  schema.corrupted_cdr might get very large and fill up memory
