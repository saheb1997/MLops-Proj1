import os 
import sys

from pandas import DataFrame
from sklearn.model_selection import train_test_split

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.exception import MyException
from src.logger import logging
from src.data_access.proj1_data import proj1Data

class DataIngestion:
    def __init__(self, data_ingestion_config:DataIngestionConfig = DataIngestionConfig()):
        '''
        :param data_ingestion_config configurartion for data ingestion
        '''

        try :
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise MyException (e,sys)
        
    def export_data_into_feature_store(self)->DataFrame:
        """
        Method Name: export_data_into_feature_store
        Description: This method export data from mongodb to csv file
        
        output: data is returned as artifact of data ingestion component
        on faillure: Write an exception log and then raise an exception
        """

        try :
            logging.info(f"exporting data from mongodb")
            my_data= proj1Data()
            dataframe = my_data.export_collectio_as_dataframe(collection_name=self.data_ingestion_config.collection_name)
            logging.info(f"sahpe of dataframe:{dataframe.shape}")
            feature_store_file_path= self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            logging.info(f"saving export data info feature store file path :{feature_store_file_path}")
            dataframe.to_csv(feature_store_file_path,index=False, header=True)
        except MyException as e:
            raise MyException(e, sys)
        
    def split_data_as_train_test(self,dataframe :DataFrame) ->None:
        """
        Method  Name    : split_data_as_train_test
        Description     : This method splits the dataframe into train set and test set base on split ratio
        
        OutPut          : Folder is created in s3 Bucket
        On Failure      : Write an exception log and then raise an exception
        """
        logging.info("Enter to the split_data_train_test function")

        try:
            train_set,test_set =train_test_split(dataframe,test_size=self.data_ingestion_config.train_test_split_ratio)
            logging.info("perfrom test_test on dataframe")
            logging.info("Exited split_data_as_train_test method of Data_ingestion class")
            dir_path = os.path.dirname(self.data_ingestion_config.traning_file_path)
            os.makedirs(dir_path ,exist_ok=True)
            logging.info(f"Exporting train and tst file path")
            train_set.to_csv(self.data_ingestion_config.traning_file_path,index=False,header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path,index=False, header = True)
            logging.info(f"exported train and test file path")
            
        except Exception as e:
            raise MyException (e,sys) from e





    def intiat_data_ingestion(self) ->DataIngestionArtifact:
        """
        Method Name : Intiate_data_ingestion
        Description : This method intiate the data ingestion components of traning pipeline
        
        Output      : train set and test set are returned as the artifact of data ingestion artifact
        On Faiure   : Write an exception log and then raise an expection
        """
        logging.info("enter the intiate_data_ingestion_part")
        try :
            dataframe =self.export_data_into_feature_store()
            logging.info("Got the data from mongodb")
            self.split_data_as_train_test(dataframe)
            logging.info("perfrom train test split on the dataset")

            logging.info(
                "Exited initiate_data_ingestion method of Data_ingestion class"

            )
            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.traning_file_path,test_file_path=self.data_ingestion_config.testing_file_path)
            logging.info("data ingestion artifact:{data_ingestion_artifact}")
            return data_ingestion_artifact
        except Exception as e:
            raise MyException (e,sys) from e


