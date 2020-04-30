import configparser

cfg = configparser.ConfigParser()

cfg.read('config.cfg')

oracle_username = cfg.get("oracle","username")
oracle_password = cfg.get("oracle","password")
oracle_host = cfg.get("oracle","host")
oracle_port = cfg.get("oracle","port")
oracle_db = cfg.get("oracle","db")
encoding = cfg.get("oracle","encoding")
oracle_table = cfg.get("oracle","table")
oracle_pool_size = cfg.get("oracle","pool_size")
insert_count  = int(cfg.get("oracle","insert_count"))
max_threads = cfg.get("oracle","max_threads")

if max_threads == "auto" :
    max_threads = None
else:
    max_threads = int(max_threads)




mongo_url = str(cfg.get("mongo","host")) + ":" + str(cfg.get("mongo","port"))
mongo_db = cfg.get("mongo","db")
mongo_collection = cfg.get("mongo","collection")

condition = cfg.get("condition","type")

if condition == "logout_time" :
    start_logout_time = cfg.get("condition","start_logout_time")
    stop_logout_time = cfg.get("condition","stop_logout_time")

elif condition == "connection_id" :
    start_connection_id = cfg.get("condition","start_connection_id") 
    stop_connection_id = cfg.get("condition","stop_connection_id")

else:
    print("condition not satisfied")
    exit()
