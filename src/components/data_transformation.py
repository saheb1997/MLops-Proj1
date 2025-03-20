import sys
import numpy as np
import pandas as pd
from imblearn.combine import SMOTEENN
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler,MinMaxScaler
from sklearn.compose import ColumnTransformer

from src.exception import MyException
from src.constants import TARGET_COLUMN ,SCHEMA_FILE_PATH, CURRENT_YEAR
from src.entity.config_entity import DataTransformation
from src.entity.artifact_entity import DataIngestionArtifact,DataTransformationArtifact,DataValidationArtifact
from src.logger import logging
from src.utils.main_utils import read_yaml_file,save_object,save_numpy_array_data

class DataTransformation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_transformation_config:DataTransformation,
                 data_validation_artifact:DataValidationArtifact):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_transformation_config = data_transformation_config
            self.data_validation_artifact = data_validation_artifact
            self._schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise MyException(e,sys) from e
        
    @staticmethod
    def read_data(file_path:str)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise MyException(e,sys) from e
    
    def get_data_transformation_object(self) ->Pipeline:
        try:
            numerical_transformer = StandardScaler()
            min_max_scaler = MinMaxScaler()
            logging.info("Creating column transformer object")

            #load schema configuration
            num_features = self._schema_config["num_features"]
            mm_columns = self._schema_config["mm_columns"]
            logging.info("Transformers Initialized: StandardScaler, MinMaxScaler")

            #creating preprocessor pipeline
            preprocessor = ColumnTransformer(
                transformers=[
                    ("standard_scaler", numerical_transformer, num_features),
                    ("min_max_scaler", min_max_scaler, mm_columns)
                ],
                remainder="passthrough"
            )
            final_pipeline = Pipeline(steps=[('preprocessor', preprocessor)])
            logging.info("Created final pipeline object")
            logging.info("Exited get_data_transformation_object method of DataTransformation class")
            return final_pipeline
        except Exception as e:  
            raise MyException(e,sys) from e
    
    def _map_gender_column(self,df):
        logging.info("mapping Gender column to binary values")
        df['Gender'] = df['Gender'].map({'Female': 0, 'Male': 1}).astype(int)
        return df
    def _create_dummy_columns(self,df):
        logging.info("Creating dummy columns for categorical columns")
        df = pd.get_dummies(df, drop_first=True)
        return df
    
    def _rename_columns(slef,df):
        logging.info("Renaming columns")
        df=df.rename(columns={
            "vehicle_age_< 1 Year":"vehicle_age_lt_1_Year",
            "vehicle_age_> 2 Years":"vehicle_age_gt_2_Years",
        })
        for col in ["vehicle_Age_lt_1_Year","vehicle_age_gt_2_Years","Vehicle_Damage_Yes"]:
            if col  in df.columns:
                df[col]= df[col].astype(int)
        return df
    
    def _drop_id_column(self,df):
        logging.info("Dropping ID column")
        drop_col = self._schema_config["drop_columns"]
        if drop_col in df.columns:
            df=df.drop(drop_col,axis=1)
        return df
    
    def initiate_data_transfomation(self) ->DataTransformationArtifact:
        """
        Method Name : initiate_data_transformation
        Description : This method is used to initiate data transformation
        Output      : DataTransformationArtifact
        On Failure  : Raise MyException
        """
        try:
            logging.info("Data Transfomation started !!!")
            if not self.data_validation_artifact.validattion_status:
                raise MyException("Data Validation is not done")

            # Load train and test data
            train_df = self.read_data(file_path=self.data_ingestion_artifact.trained_file_path)
            test_df = self.read_data(file_path=self.data_ingestion_artifact.test_file_path)
            logging.info("Read train and test data")


            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN],axis=1)
            test_df = self.read_data(file_path=self.data_ingestion_artifact.test_file_path) 
            logging.info("Read train and test data")

            input_feature_train_df = test_df.drop(columns = [TARGET_COLUMN],axis=1)
            target_train_df = test_df[TARGET_COLUMN]
            logging.info("Split input feature and target column")
        except Exception as e:
            raise MyException(e,sys) from e



            
