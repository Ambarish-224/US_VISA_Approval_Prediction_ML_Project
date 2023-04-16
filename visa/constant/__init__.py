import os
from datetime import datetime


def get_current_time_stamp():
    return f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

CURRENT_TIME_STAMP = get_current_time_stamp()

ROOT_DIR = os.getcwd()  #to get current working directory
CONFIG_DIR = "config"
CONFIG_FILE_NAME = "config.yaml"
CONFIG_FILE_PATH = os.path.join(ROOT_DIR,CONFIG_DIR,CONFIG_FILE_NAME)


# Data Ingestion related variable
DATA_INGESTION_CONFIG_KEY = "data_ingestion_config"
DATA_INGESTION_DOWNLOAD_URL_KEY = "dataset_download_url"
DATA_INGESTION_ARTIFACT_DIR = "data_ingestion"
DATA_INGESTION_RAW_DATA_DIR_KEY = "raw_data_dir"
DATA_INGESTION_INGESTED_DIR_NAME_KEY = "ingested_dir"
DATA_INGESTION_TRAIN_DIR_KEY = "ingested_train_dir"
DATA_INGESTION_TEST_DIR_KEY = "ingested_test_dir"

# Training pipeline related variable
TRAINING_PIPELINE_CONFIG_KEY = "training_pipeline_config"
TRAINING_PIPELINE_ARTIFACT_DIR_KEY = "artifact_dir"
TRAINING_PIPELINE_NAME_KEY = "pipeline_name"


# Company Variable
COLUMN_COMPANY_AGE = "company_age"
COLUMN_YEAR_ESTB = "yr_of_estab"
COLUMN_ID = "case_id"
COLUMN_CASE_STATUS = "case_status"

# Data Validation related variable
DATA_VALIDATION_ARTIFACT_DIR="data_validation"
DATA_VALIDATION_CONFIG_KEY = "data_validation_config"
DATA_VALIDATION_SCHEMA_FILE_NAME_KEY = "schema_file_path"
DATA_VALIDATION_SCHEMA_DIR_KEY = "schema_dir"

# Data Transformation related variable
DATA_TRANSFORMATION_CONFIG_KEY = "data_transformation_config"
DATA_TRANSFORMATION_ARTIFACT_DIR = "data_transformation"
DATA_TRANSFORMATION_DIR_NAME_KEY = "transformed_dir"
DATA_TRANSFORMATION_TRAIN_DIR_NAME_KEY = "transformed_train_dir"
DATA_TRANSFORMATION_TEST_DIR_NAME_KEY = "transformed_test_dir"
DATA_TRANSFORMATION_PREPROCESSING_DIR_KEY = "preprocessing_dir"
DATA_TRANSFORMATION_PREPROCESSED_FILE_NAME_KEY = "preprocessed_object_file_path"

TARGET_COLUMN_KEY = "target_column"
DATASET_SCHEMA_COLUMNS_KEY = "ColumnNames"

NUMERICAL_COLUMN_KEY = "Numerical_columns"
ONE_HOT_COLUMN_KEY = "Onehot_columns"
ORDINAL_COLUMN_KEY = "Oridnal_columns"
TRANSFORM_COLUMN_KEY = "Transformation_columns"

# Model Training Variable
# Model Training related variable
MODEL_TRAINER_ARTIFACT_DIR = "model_trainer"
MODEL_TRAINER_CONFIG_KEY = "model_trainer_config"
MODEL_TRAINER_TRAINED_MODEL_DIR_KEY = "trained_model_dir"
MODEL_TRAINER_TRAINED_MODEL_FILE_NAME_KEY ="model_file_name"
MODEL_TRAINER_BASE_ACCURACY_KEY = "base_accuracy"
MODEL_TRAINER_MODEL_CONFIG_DIR_KEY ="model_config_dir"
MODEL_TRAINER_MODEL_CONFIG_FILE_NAME_KEY ="model_config_file_name"


# Model Validation variable

# Model Pusher Varable