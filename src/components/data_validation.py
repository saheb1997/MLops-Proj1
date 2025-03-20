import json 
import sys
import os

import pandas as pd
from pandas import DataFrame

from src.exception import MyException
from src.logger import logging
from src.utils.main_utils import read_yaml_file
from src.entity.artifact_entity import DataValidationArtifact,DataIngestionArtifact
from src.constants import SCHEMA_FILE_PATH
from src.entity.config_entity import DataValidationConfig


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        """
        :param data_ingestion_artifact: Output reference of data ingestion artifact stage
        :param data_validation_config: configuration for data validation
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config =read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise MyException(e,sys)
        
    def validation_number_of_columns(self, dataframe: DataFrame) -> bool:
        try:
            status = len(dataframe.columns) == len(self._schema_config['columns'])
            logging.info(f"Number of columns validation status: {status}")
            return status
        except Exception as e:
            raise MyException(e,sys)
    
        
    def is_column_exist(self, df: DataFrame) ->bool:
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            if len(missing_numerical_columns) > 0:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")

            for column in self._schema_config["categorical_columns"]:
                if column not in dataframe_columns:
                    missing_categorical_columns.append(column)
            if(len(missing_categorical_columns) > 0):
                logging.info(f"Missing categorical columns: {missing_categorical_columns}")
           
        
        except Exception as e:  
            raise MyException(e,sys)
        
    @staticmethod
    def read_data(file_path:str)->DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise MyException(e,sys)
        
    def initiate_data_validation(self)->DataValidationArtifact:
        """
        Method Name : initiate_data_validation
        Description : This method is used to initiate data validation
        Output      : DataValidationArtifact
        On Failure  : Raise MyException
        """
        
        try:
            validation_error_msg =""
            logging.info("Initiating data validation")
            train_df ,test_df =(DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                                DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path))
            logging.info("Read train and test data")
            status = self.validation_number_of_columns(dataframe=train_df)
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe. "
            else:
                logging.info(f"All required columns present in training dataframe: {status}")

            status = self.validation_number_of_columns(dataframe=test_df)
            if not status:
                validation_error_msg += f"Columns are missing in testing dataframe. "
            else:
                logging.info(f"All required columns present in testing dataframe: {status}")

            #validation col type for train and test data
            status = self.is_column_exist(df=train_df)
            if not status:
                validation_error_msg += f"Missing columns in training dataframe. "
            else:
                logging.info(f"All required columns present in training dataframe: {status}")


            status = self.is_column_exist(df=test_df)
            if not status:
                validation_error_msg += f"Missing columns in testing dataframe. "
            else:
                logging.info(f"All required columns present in testing dataframe: {status}")
            
            validation_status = len(validation_error_msg) == 0
            logging.info(f"Validation status: {validation_status}")

            data_validation_artifact = DataValidationArtifact(validattion_status=validation_status,
                                                              message=validation_error_msg,
                                                              validation_report_file_path =self.data_validation_config.validation_report_file_path)
            logging.info(f"Data validation artifact: {data_validation_artifact}")

            #Ensure the directory is created
            report_dir = os.path.dirname(self.data_validation_config.validation_report_file_path)
            os.makedirs(report_dir,exist_ok=True)

            #save validation status and message to json file
            validation_report = {
                "validation_status":validation_status,
                "message":validation_error_msg
            }
            with open(self.data_validation_config.validation_report_file_path, 'w') as file:
                json.dump(validation_report,file)
            logging.info("Data validation artifact created and saved to JSON file.")
            logging.info("Data validation arifact: {data_validation_artifact}")

        except Exception as e:
            raise MyException(e,sys)  from e




            

            
            
    