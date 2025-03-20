import os 
import sys
 
import numpy as np
import dill
import yaml
from pandas import DataFrame

from src.exception import MyException
from src.logger import logging

def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise MyException(e,sys) from e
    
def write_yaml_file(file_path: str ,content: object ,replace : bool =False):
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as yaml_file:
            yaml.dump(content, yaml_file, default_flow_style=False)
    except Exception as e:
        raise MyException(e,sys) from e
    
def lod_object(file_path:str)->object:
    try:
        with open(file_path, 'rb') as file:
            return dill.load(file)
    except Exception as e:
        raise MyException(e,sys) from e

def save_numpy_array_data(file_path:str , array :np.array):

    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path,exist_ok=True)
        with open(file_path, 'wb') as file:
            np.save(file,array)
    except Exception as e:  
        raise MyException(e,sys) from e
    
def save_object(file_path:str , obj:object)->None:
    logging.info(f"Saving object at {file_path}")
    try:
        os.mkdir(os.path.dirname(file_path),exist_ok=True)
        with open(file_path, 'wb') as file:
            dill.dump(obj,file)
    except Exception as e:
        raise MyException(e,sys) from e



    
