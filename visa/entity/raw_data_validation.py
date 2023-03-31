from visa.exception import CustomException
from visa.logger import logging
import os, sys
from visa.utils.utils import read_yaml_file
import pandas as pd
import collections


class IngestedDataValidation:

    def __init__(self, validate_path, schema_path):
        try:
            self.validate_path = validate_path
            self.schema_path = schema_path
            self.data = read_yaml_file(self.schema_path)
        except Exception as e:
            raise CustomException(e,sys) from e

    def validate_filename(self, file_name)->bool:
        try:
            print(self.data["FileName"])
            schema_file_name = self.data['FileName']
            if schema_file_name == file_name:
                return True
        except Exception as e:
            raise CustomException(e,sys) from e

    def validate_column_length(self)->bool:
        try:
            df = pd.read_csv(self.validate_path)
            if(df.shape[1] == self.data['NumberofColumns']):
                return True
            else:
                return False
        except Exception as e:
            raise CustomException(e,sys) from e

    def missing_values_whole_column(self)->bool:
        try:
            df = pd.read_csv(self.validate_path)
            count = 0
            for columns in df:
                if (len(df[columns]) - df[columns].count()) == len(df[columns]):
                    count+=1
            return True if (count == 0) else False
        except Exception as e:
            raise CustomException(e,sys) from e

    def replace_null_values_with_null(self)->bool:
        try:
            df = pd.read_csv(self.validate_path)
            df.fillna('NULL',inplace=True)
        except Exception as e:
            raise CustomException(e,sys) from e

    
    def check_column_names(self)->bool:
        try:
            df = pd.read_csv(self.validate_path)
            df_column_names = df.columns
            schema_column_names = list(self.data['ColumnNames'].keys())

            return True if (collections.Counter(df_column_names) == collections.Counter(schema_column_names)) else False

        except Exception as e:
            raise CustomException(e,sys) from e