import pandas as pd
import os
import re
import numpy as np
from datetime import date, datetime
import pyarrow.parquet as pq

class audit_logger:
    # funtion for logging files which are avilable in control files but not in source files
    def log_source(self,prefix,valid_file): 
        valid_df = pd.read_json(valid_file) 
        date_string = date.today().strftime("%Y%m%d")
        # create a path to store the log file
        log_src = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\ingestion_log_' + date_string + '.csv'
        # create dict containing file content
        data = {
            'file_name': prefix + '_' + date_string + '.csv',
            'file_date': date_string,
            'expected_rows': valid_df['expected_rows'][0],
            'actual_rows': 0,
            'status': 'Failed',
            'ingestion_date': datetime.now().isoformat()
        }

        df = pd.DataFrame([data]) # convert dict to df

        # save df as log csv file with header as false if file exists and header as true if file doesnt exists
        df.to_csv(
            log_src,
            mode='a',
            index=False,
            header=not os.path.exists(log_src)
        ) 
    
    # function for logging files which are being through validation pipeline
    def log_ingestion(self,filename , status, actual_rows, expected_rows):
        datestr = datetime.now().isoformat()
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
    
    
    # funtion for logging files from preprocessing pipeline
    def log_preprocess(self,filename,duplicates):
        datestr = datetime.now().isoformat()
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
    
    # function for logging retention files from retention pipeline
    def log_retention(self,file_path,zip_file):
        datestr = datetime.now().isoformat()
        date_string = date.today().strftime("%Y%m%d")
        dict = {
            'file_name': file_path,
            'archived_on':datestr,
            'location': zip_file
        }
        df = pd.DataFrame([dict])
        log_retain = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\retention_log_'+date_string+'.csv'
        df.to_csv(log_retain, index = False , mode = 'a', header = not os.path.exists(log_retain))

   
           