from schema import Schema
from connection import OracleConnection,MongoConnection
import redis
from log import main_logger
import datetime
import config
import threading
import uuid

# try to connect to redis server
try:
    r6 = redis.StrictRedis(host="localhost",port=6379,db=6)
    r6.ping()
    r6.set("start",str(datetime.datetime.now()))
except Exception  as e:
    print("redis - connection refused")
    main_logger.error("redis - connection refused")
    exit()


# CAUSION : 
# each thread gets 8MB stack of your memory to address
# if you want to use more threads be carefull 
# HINT: you can use pthread_attr_setstacksize() to reduce the size of thread stacks (threading.stacksize())
MAX_THREAD = config.max_threads
INSERT_COUNT = config.insert_count
oc = OracleConnection(autocommit=True)
cursor  = oc.get_cursor()
cursor.execute("select column_name, data_type from all_tab_columns where table_name = 'CONNECTION_LOG'")

with open('evaluate.txt','w') as f:
    for column,data_type in cursor.fetchall():
        if data_type.startswith('NUMBER'):
            data_type = 'int'
        elif data_type.startswith('VARCHAR'):
            data_type = 'str'
        elif data_type.startswith('DATE'):
            data_type = 'datetime'
        f.write("{} {}\n".format(column,data_type))

cursor.execute("select column_name from all_tab_columns where table_name = 'CONNECTION_LOG' and nullable = 'N' ")

mandator_fields = []
for column in cursor.fetchall():
    mandator_fields.append(*column)


def check_threads(threads):

    current_threads = len(threads)
    if  current_threads >= MAX_THREAD :
        main_logger.debug("number of threads exceeded")
        main_logger.debug("waiting to release the some threads...")
        release_threads = int(current_threads/2)
        for i in range(release_threads):
            if i is threading.current_thread():
                continue
            th = threads.pop(0)
            th.join()
            main_logger.debug(f"thread {th} released successfully")



schema = Schema("schema.txt","evaluate.txt",mandator_fields)
mongo = MongoConnection()

main_logger.info("database drivers initiated successfully ...")


docs = mongo.get_docs()
threads = []
oracle_rows = []
docs_count = docs.count()


for doc in docs:
    data = schema.pre_processing(doc)
    if data:
        oracle_rows.append(schema.checkout(data))
        if len(oracle_rows) % INSERT_COUNT == 0:
            check_threads(threads)
            t = threading.Thread(target=oc.insert_many,args=(oracle_rows,),name=str(uuid.uuid1()))
            threads.append(t)
            t.start()
            oracle_rows = []
            
if len(oracle_rows) > 0 :
    t = threading.Thread(target=oc.insert_many,args=(oracle_rows,),name=str(uuid.uuid1()))
    threads.append(t)
    t.start()
    


#TODO: Insert connection log ids to disk



# make sure all of the threads are done before close the connections
for th in threads:
    th.join()

del oc
mongo.__del__()

r6.set("stop",str(datetime.datetime.now()))