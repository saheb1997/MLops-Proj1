from src.logger import logging

# logging.debug('This is a debug message')
# logging.error('This is a error message')    
# logging.info('This is a info message')
# logging.warning('This is a warning message')
# logging.critical('This is a critical message')

# from src.exception import MyException
# import sys

# try:
#     a=1+'z'
# except Exception as e:
#     logging.info(e)
#     raise MyException() from e
# from src.pipline.training_pipeline import TrainPipeline

# pipeline= TrainPipeline()
# pipeline.run_pipeline()from pymongo import MongoClient
import pymongo

MONGODB_URI = "mongodb+srv://sahebs450:q9E1eLoQjWSQn8LU@cluster0.xx8ry.mongodb.net/?retryWrites=true&w=majority"

client = pymongo.MongoClient(MONGODB_URI)

# Replace 'your_database_name' with the actual database name
db = client["Proj1"]

# Fetch and print collections
print("Collections:", db.list_collection_names())
