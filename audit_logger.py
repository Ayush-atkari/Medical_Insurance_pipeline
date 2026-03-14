import pandas as pd
import os
import re
import numpy as np
from datetime import date, datetime
import pyarrow.parquet as pq

class audit_logger:
    def log_srcToPreprocess(self, preprocessedPath):
        df = pd.read_parquet(preprocessedPath)
        SOURCE_PATH = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\ingestion'
        filename = preprocessedPath.split('.')[0]+ '.csv'
        logs = []
        for column in df.columns:
            logs.append({
                'SOURCE_LAYER':'Source',
                'SOURCE_FILE': filename,
                'COLUMN_NAME' : column,
                'DATA_TYPE' : df[column].dtype,
                'BUSINESS_LOGIC': ('date time' if column=='ingestion_date' else'File date appended to File name' if column == 'file_date' else 'Direct Mapping'),
                'TARGET_LAYER': 'Preprocessed',
                'TARGET_FILE': preprocessedPath,
                'DATA_TYPE':str(df[column].dtype),
                'LOAD_TYPE':'Truncate and Load'
            })
        log_df = pd.DataFrame(logs)
        log_df.to_csv(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\STTM_srcToPreprocess.csv',mode='a',header = False , index = False)
        
    def log_source(self,prefix,valid_file):
        valid_df = pd.read_json(valid_file)
        date_string = date.today().strftime("%Y%m%d")

        log_src = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\ingestion_log_' + date_string + '.csv'

        data = {
            'file_name': prefix + '_' + date_string + '.csv',
            'file_date': date_string,
            'expected_rows': valid_df['expected_rows'][0],
            'actual_rows': 0,
            'status': 'Failed',
            'ingestion_date': datetime.now()
        }

        df = pd.DataFrame([data])

        df.to_csv(
            log_src,
            mode='a',
            index=False,
            header=not os.path.exists(log_src)
        )
    
    def log_ingestion(self,filename , status, actual_rows, expected_rows):
        datestr = datetime.now()
        date_string = date.today().strftime("%Y%m%d")
        dict = {
            'file_name': filename,
            'file_date': filename.split('_')[1].split('.')[0],
            'expected_rows': expected_rows,
            'actual_rows': actual_rows,
            'status': status,
            'ingestion_date': datestr
        }
        df = pd.DataFrame([dict])
        log_src = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\ingestion_log_'+ date_string +'.csv'
        df.to_csv(log_src, index=False , mode = 'a', header = not os.path.exists(log_src))
    
    def log_preprocess(self,filename,duplicates):
        datestr = datetime.now()
        date_string = date.today().strftime("%Y%m%d")
        dict = {
            'file_name': filename,
            'load_id': filename + '_'+date_string,
            'invalid_rows': duplicates,
            'duplicates_removes': duplicates,
            'preprocessed_time': datestr
        }
        df = pd.DataFrame([dict])
        log_path = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\preprocess_log_'+ date_string +'.csv'
        df.to_csv(log_path, index=False , mode = 'a', header = not os.path.exists(log_path))
    
    def log_retention(self,file_path,zip_file):
        datestr = datetime.now()
        date_string = da
        dict = {
            'file_name': file_path,
            'archived_on':datestr,
            'location': zip_file
        }
        df = pd.DataFrame([dict])
        log_retain = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\retention_log_'+datestr.strftime('%Y%m%d')+'.csv'
        df.to_csv(log_retain, index = False , mode = 'a', header = not os.path.exists(log_retain))
        
           