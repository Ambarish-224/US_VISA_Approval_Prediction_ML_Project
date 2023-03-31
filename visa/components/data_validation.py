import os
import sys
from visa.constant import *
from visa.logger import logging
from visa.entity.config_entity import DataValidationConfig
from visa.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from visa.config.configuration import Configuartion
from visa.exception import CustomException
from visa.utils.utils import read_yaml_file
from visa.entity.raw_data_validation import IngestedDataValidation

class DataValidation:

    def __init__(self, data_validation_config: DataValidationConfig,
                 data_ingestion_artifact: DataIngestionArtifact):
        try:
            logging.info(
                f"{'>>' * 30}Data Validation log started.{'<<' * 30} \n\n")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.schema_path = self.data_validation_config.schema_file_path
            self.train_data = IngestedDataValidation(
                validate_path=self.data_ingestion_artifact.train_file_path, schema_path=self.schema_path)
            self.test_data = IngestedDataValidation(
                validate_path=self.data_ingestion_artifact.test_file_path, schema_path=self.schema_path)
        except Exception as e:
            raise CustomException(e, sys) from e

    def isFolderPathAvailable(self) -> bool:
        try:

             # True means avaliable false means not avaliable
             
            isfolder_available = False
            train_path = self.data_ingestion_artifact.train_file_path
            test_path = self.data_ingestion_artifact.test_file_path
            if os.path.exists(train_path):
                if os.path.exists(test_path):
                    isfolder_available = True
            return isfolder_available
        except Exception as e:
            raise CustomException(e, sys) from e

    def is_Validation_successfull(self):
        try:
            validation_status = True
            logging.info("Validation Process Started")
            if self.isFolderPathAvailable() == True:
                train_filename = os.path.basename(
                    self.data_ingestion_artifact.train_file_path)

                is_train_filename_validated = self.train_data.validate_filename(
                    file_name=train_filename)

                is_train_column_numbers_validated = self.train_data.validate_column_length()

                is_train_column_name_same = self.train_data.check_column_names()

                is_train_missing_values_whole_column = self.train_data.missing_values_whole_column()

                self.train_data.replace_null_values_with_null()

                test_filename = os.path.basename(
                    self.data_ingestion_artifact.test_file_path)

                is_test_filename_validated = self.test_data.validate_filename(
                    file_name=test_filename)

                is_test_column_numbers_validated = self.test_data.validate_column_length()

                is_test_column_name_same = self.test_data.check_column_names()

                is_test_missing_values_whole_column = self.test_data.missing_values_whole_column()

                self.test_data.replace_null_values_with_null()

                logging.info(
                    f"Train_set status|is Train filename validated?: {is_train_filename_validated}|is train columns validated?: {is_train_column_numbers_validated}|is train column name validated?: {is_train_column_name_same}|whole missing columns?{is_train_missing_values_whole_column}")
                logging.info(
                    f"Test_set status|is Test filename validated?: {is_test_filename_validated}is test col numbers validated?: {is_test_column_numbers_validated}|is test column names validated? {is_test_column_name_same}| whole missing columns? {is_test_missing_values_whole_column}")

                if is_train_filename_validated & is_train_column_numbers_validated & is_train_column_name_same & is_train_missing_values_whole_column:
                    pass
                else:
                    validation_status = False
                    logging.info("Check yout Training Data! Validation Failed")
                    raise ValueError(
                        "Check your Training data! Validation failed")

                if is_test_filename_validated & is_test_column_numbers_validated & is_test_column_name_same & is_test_missing_values_whole_column:
                    pass
                else:
                    validation_status = False
                    logging.info("Check your Test data! Validation failed")
                    raise ValueError(
                        "Check your Testing data! Validation failed")

                logging.info("Validation Process Completed")

                return validation_status

        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_data_validation(self):
        try:
            data_validation_artifact = DataValidationArtifact(
                schema_file_path=self.schema_path, is_validated=self.is_Validation_successfull(),
                message="Data validation performed"
            )
            logging.info(
                f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys) from e

    def __del__(self):
        logging.info(f"{'>>' * 30}Data Validation log completed.{'<<' * 30}")