import logging
import os

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

if not os.path.exists("logs"):
    os.makedirs("logs")


def setup_logger(name,log_file,level=logging.DEBUG):

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


main_logger = setup_logger('main_logger', 'logs/main.log')
connection_logger = setup_logger('connection_logger','logs/connection.log')
schema_logger = setup_logger('schema_logger','logs/schema.log')

