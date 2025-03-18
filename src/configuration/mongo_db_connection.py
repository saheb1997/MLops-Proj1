import os
import sys
import pymongo
import certifi

from src.exception import MyException
from src.logger import logging
from src.constants import DATABASE_NAME , MONGODB_URL_KEY

ca = certifi.where()

class MongoDBClient:
    '''
    MongoDBClient is responsible for establish a connection to the mongoDB database

    Attributes:
    ------------
    client : MongoClient
        A shared MongoClient instance for the class
    databse: DataBase
        The specific database instance that MongoDBClient connects to.
    
    Methods:
    -------------
    __init__(datas_name: str) ->None
        Initializes the mongoDb connection using the given database name.
    '''

    client =None

    def __init__(self, database_name: str =DATABASE_NAME) ->None:
        '''
        Initializes a connection to the MongoDB database. If no existing connection is found it establish a new one.
        Parameters:
        -----------
        database_name : str, optional
            Name of the MongoDB database to connect to.Default is set by Database_name constant.
        
        Raises:
        -----------
        My Excpetion
         If there is an issue connection to MongoDB or if the environment varialble for the mongoDB url is not set.
         '''
        try:
            #check if mongoDB connection already build or not if not build a new one.
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception("Environment variable '{MONGODB_URL_KEY}' is not set.")
                MongoDBClient.client =pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
            
            #use the shared MongoClient for this instance

            self.client = MongoDBClient.client
            self.database= self.client[database_name]
            self.database_name= database_name
            logging.info("MongoDB connection successfull")
        except Exception as e:
            #raise a custom exception with traceback details if connection fails
            raise MyException(e, sys)


    