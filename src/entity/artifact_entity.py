from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    trained_file_path:str
    test_file_path :str

@dataclass
class DataValidationArtifact:
    validattion_status:bool
    validation_report_path:str

@dataclass
class DataTransformationArtifact:
    transformed_train_file_path:str
    transformed_test_file_path:str
    transformed_object_file_path:str