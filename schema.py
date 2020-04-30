from datetime import datetime
import collections
from log import schema_logger
import redis


class Schema:

    def __init__(self,schema_path="SCHEMA",validation_path="EVALUATE",mendatory_fields=None):

        self.order_dict = collections.OrderedDict()
        self.schema_path = schema_path
        self.validation_path = validation_path
        self.schema = {}
        self.schema_loader()
        self.validation_loader()
        self.mandatory_fields = mendatory_fields
        self.initiate_redis()
        self.corrupted_cdr = []
    
    def schema_loader(self):
        """
            this function load schema file that contains oracle columns and mongo relevent fields

        """
        try:
            with open(self.schema_path) as f:
                for line in f:
                    (key,value) =  line.split()
                    self.schema[key] = value
        except:
            schema_logger.error("schema doesn't load correctly \n  \
                filepath=%s" %(self.schema_path))

    def initiate_redis(self):

        self.r5 = redis.StrictRedis(host="localhost",port=6379,db=5)

    def validation_loader(self):
        """
            this function load evaluate file that contains oracle columns and data types 
            evaluate file should be sorted based on columns 

        """
        try:
            with open(self.validation_path) as f:
                for line in f:
                    (key,value) = line.split()
                    self.order_dict[key] = eval(value)
        except:
            schema_logger.error("validation doesn't load correctly \n  \
                filepath=%s" %(self.validation_path))

    def check_mandatory_fields(self,data):
        """
            this function check the mandatory fields be satisfied for nullable columns 

        """
        for key in self.mandatory_fields:
            if data.get(key) is  None:
                try:
                    cdr_id = data.get("CONNECTION_LOG_ID")
                    if cdr_id:
                        self.r5.set(f'CONNECTION_LOG_ID||{cdr_id}','corrupted!')
                except:
                    schema_logger.error("couldn't extract connection_id in corrupted instance")
                return False
        return True

    def pre_processing(self,doc):
        """
            :params
                    doc (dict) -> mongo document fetched without any processing
            :returns
                    new_doc (dic) -> new dictionary changed key names of oracle column's name

            this function will map data fields and create a new document with new mapped fields
        """
        new_doc = {}
        try:
            details = doc.pop("details")
        except:
            schema_logger.error("couldn't extract [details] from document")
            
        try:
            type_details = doc.pop("type_details")
        except:
            schema_logger.error("couldn't extract [type_details] from document")
            
        try:
            charge_rule_details  = doc.pop("charge_rule_details")
        except:
            schema_logger.error("couldn't extract [charge_rule_details] from document")
            
        
        # Default Values
        new_doc["CREDIT_INDEX"] = 1
        new_doc["HOTLINE_SESSION"] = 0
        new_doc["SESSION_BEFORE_STOP_UNCOMMITTED_CREDIT"]  =0 
        new_doc["BEARER_START_TIME"] = datetime.now()
        new_doc["SESSION_START_TIME"] = datetime.now()
        new_doc["SESSION_STOP_TIME"] = datetime.now()


        # nested fields
        try:
            if len(charge_rule_details) > 0:
                new_doc["CHARGE_ID"] = charge_rule_details[0]["charge_rule_id"]
                new_doc["CHARGE_RULE"] = charge_rule_details[0]["charge_rule_desc"]
            else:
                new_doc["CHARGE_ID"] =  0
        except:

            schema_logger.error("charge_rule_details not found")

        if details:
            try:
                new_doc["MAC"] = details.get("mac")
                new_doc["PARENT_SESSION_ID"] = details.get("parent_session_id")
                new_doc["SESSION_PORT"] = details.get("port")
            except:

                schema_logger.error("details not found")

        if type_details:
            try : 
                new_doc["IPV4_ADDRESS"] = type_details.get("remote_ip")
                new_doc["SUBSERVICE_CHARGING"] = type_details.get("sub_service_charging")
                new_doc["SUBSERVICE_NAME"] = type_details.get("sub_service_name")
                new_doc["SUBSERVICE_QOS"] = type_details.get("sub_service_qos")
            except:
                schema_logger.error("type_details not found")


        # flat fields
        for key,value in doc.items():
            if key in self.schema.keys():
                new_doc[self.schema[key]] = value
        
        # check all the mandatory fields to satisfied
        if self.check_mandatory_fields(new_doc):
            return new_doc

        return None

    def checkout(self,data):
        """
            this function is the last step of preparing data for insert to oracle
            in this function data will sort  base on oracle's columns and validate the column's data type

            :params
                data (dict) 
            :returns
                new_data (tuple)

        """
        new_data = {}
        for key in self.order_dict.keys():
            value = data.get(key)

            if  isinstance(value,float) :
                value = int(value)
            
            if isinstance(value,self.order_dict[key]) or value is None:
                new_data[key] = value
            else:
                new_data[key] = None

        
        return new_data
