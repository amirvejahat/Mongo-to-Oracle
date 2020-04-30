import config
from pymongo import MongoClient
import pymongo
from datetime import datetime
import cx_Oracle
from log import connection_logger,main_logger
import threading

class OracleConnection:
    instance = None

    def __init__(self,arraysize=100000,autocommit=False):
        """
            :params
                :arraysize (int)         size of connection array
                :autocommit (bool)  commit transaction automatically

        """
        self.cursor = None
        self.arraysize = arraysize
        self.autocommit = autocommit
        self.initiate_connection()
        
    def initiate_connection(self):
            """
                this function initiate connections to oracle database and acquire sessions
            
                :params
                :returns

            """
            pool = self.connection_pool()
            if pool:
                self.pool = pool             
                self.connection = self.pool.acquire()


    def  connection_pool(self):
        """
            this function try to connect to oracle instance
            and create a pool of connections based on config.cfg file
        
            :params
            :returns
                returning a pool of connections
        
        """
        try : 
            dsn_tns = cx_Oracle.makedsn(config.oracle_host,config.oracle_port,config.oracle_db)
            pool = cx_Oracle.SessionPool(
                config.oracle_username,
                config.oracle_password,
                dsn_tns,
                min = int(config.oracle_pool_size),
                max = int(config.oracle_pool_size),
                increment = 0 ,
                encoding = "UTF-8",
                threaded=True
            )
        except cx_Oracle as error : 
            connection_logger.exception(error)
            return None

        return pool

    def get_cursor(self):
        """
            :params
            :returns
                the cursor (obj) of oracle connection
        
        """
        if self.cursor:
            return self.cursor
        try:
            self.initiate_connection()
            return self.connection.cursor()
        except cx_Oracle as error :
            connection_logger.error("couldn't create a new cursor to oracle database")
            connection_logger.exception(error)
    
    def get_new_cursor(self):
        """
            this function provide a new cursor for each thread
        """

        return self.connection.cursor()


    def insert_many(self,rows):
        """
            insert many rows simultaneously
        
            :params
                :rows (list)
                        sorted list contains tuple data

            :returns
        
        """
        
        main_logger.debug(f"thread {threading.current_thread().name} start to inserting data")
        data = rows[0]
        cols = ','.join(data.keys())
        params = ','.join(':' + str(k) for k in data.keys() )
        statement = 'insert into connection_log (' + cols + ') values (' + params + ')'
        self.cursor = self.get_new_cursor()
        self.cursor.arraysize = self.arraysize
        self.cursor.executemany(statement,
                                        rows,
                                        batcherrors=True)
        
        if hasattr(self.cursor,'getbatcherrors()'):
            for error in self.cursor.getbatcherrors():    
                connection_logger.error("%s at row offset %s" % (error.message.rstrip(),error.offset))
        
        if self.autocommit :
            self.connection.commit()
            main_logger.debug(f"thread {threading.current_thread().name} inserted successfully")
        
        
    
    def commit(self):
        """
            this function commit connection transaction 

        """
        if self.connection:
            self.connection.commit()


    def __del__(self):
        """
            this function will call before deleting instance 
            and release database sessions and close connection

        """
        self.pool.release(self.connection)
        self.pool.close()

    def __new__(cls,*args,**kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(OracleConnection)
            return cls.instance
        return cls.instance


class MongoConnection:

    instance = None

    def __init__(self,batchsize=100000):
        
        try:
            self.connection = MongoClient(config.mongo_url,readPreference='secondaryPreferred')
        except pymongo.errors as e:
            connection_logger.exception(e)

        self.batchsize  = batchsize
        self.cursor = None
        
    def get_cursor(self):

        if self.cursor:
            return self.cursor
        
        return self.connection[config.mongo_db][config.mongo_collection]


    def get_docs(self):

        connection_logger.info("fetching data from mongo...")
        if config.condition == "logout_time":
            
            s_y,s_m,s_d =  config.start_logout_time.split(',')
            e_y,e_m,e_d =  config.stop_logout_time.split(',')
            start_logout_time = datetime(int(s_y),int(s_m),int(s_d),0,0,0) 
            stop_logout_time = datetime(int(e_y),int(e_m),int(e_d),0,0,0)
            query  = {"logout_time": {"$gt" : start_logout_time , "$lt" : stop_logout_time} }
            index_name = "logout_time_-1"
        
        elif config.condition == "connection_id":
            start_connection_id = config.start_connection_id 
            end_connection_id =  config.end_connection_id
            query = {"_id" : {"$gt": start_connection_id , "$lt" : end_connection_id }}
            index_name = "connection_id"
        
        else:
            raise
        
        
        cursor = self.get_cursor()
        docs = cursor.find(query,no_cursor_timeout=True,batch_size=self.batchsize)

        return docs


    def __del__(self):
        
        self.connection.close()
        

    def __new__(cls,*args,**kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(MongoConnection)
            return cls.instance
        return cls.instance