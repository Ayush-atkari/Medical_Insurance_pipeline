from ast import pattern

import pandas as pd
import os
import numpy as np
import re
from audit_logger import audit_logger 
from datetime import date, datetime
# print("Ingestion Manager Loaded")
# path = r"C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files\agents_20251017.csv"
# df = pd.read_csv(path)
# # print(df.columns)
# # print(df.shape[0])

# metadata = pd.read_json(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files\agents_yyyymmdd.json')
# # print(metadata)

# # if df.shape[0] != metadata['expected_rows']:
# #     print('Invalid File')
# # for column in metadata['expected_columns']:
# #     for column2 in df.columns:
# #         try:
# #             if column == column2:
# #                 continue
# #         except:
# #             print('Invalid File')
        
# #     print(column)

# filename = os.path.basename(path)
# print(filename)
# name,extension = os.path.splitext(filename)
# print(name)
# print(extension)

# valid_file_name = metadata['file_name']
# print(valid_file_name[0])
# prefix = valid_file_name[0].split('_')[0]
# print(prefix)

# valid_extension = metadata['expected_extension'][0]
# print(valid_extension)

# pattern = rf"^{prefix}_\d{{8}}$"
# if not re.match(pattern,name) or extension != valid_extension or df.shape[0] != metadata['expected_rows'] or set(metadata['expected_columns']) != set(df.columns):
#     print('Invalid File Name')
# else:
#     print('Valid File Name')

class ingestion_manager:
    def is_valid(self, orgfilepath, validfilepath): # .csv filepath along with its json filepath is collected 
        status = 'Success' # defined for logging argument
        ingestionpath = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\ingestion'
        
        df = pd.read_csv(orgfilepath)
        metadata = pd.read_json(validfilepath)
        
        filename = os.path.basename(orgfilepath)
        name,extension = os.path.splitext(filename)
        
        print(f"Validating file - {filename}\n")
        
        valid_extension = metadata['expected_extension'][0]
        valid_filename = metadata['file_name'][0]
        
        # checking all the condition for validating .csv file in reference to its json file
        if filename != valid_filename or extension != valid_extension or df.shape[0] != metadata['expected_rows'][0] or set(metadata['expected_columns']) != set(df.columns):
            print(f'Invalid File: {filename}\n Filepath: {orgfilepath}\n')
            status = 'Failed'
        else:
            df.to_csv(os.path.join(ingestionpath, filename), index=False) # storing file from source_files to ingestion folder if valid.
            print(f'Valid File: {filename}\n Filepath: {orgfilepath}\n')
        
        logger = audit_logger()
        #log the files which are valid or invalid.
        logger.log_ingestion(filename , status , df.shape[0], metadata['expected_rows'][0])
