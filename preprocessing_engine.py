import pandas as pd
import os
import re
import numpy as np
from datetime import date, datetime
import pyarrow.parquet as pq
from audit_logger import audit_logger




# path = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\ingestion\policies_20251017.csv'
# df = pd.read_csv(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\ingestion\policies_20251017.csv')
# print(df.head(5))
# print(df.shape[0])
# print(df.duplicated().sum())
# df.drop_duplicates(inplace = True)
# print(df.shape[0])
# print(df.duplicated().sum())

# print(date.today())
# print(date(2025,10,17))
# filename = path.split('\\')[-1]
# datestring = filename.split('_')[1].split('.')[0]
# file_date = datetime.strptime(datestring,'%Y%m%d').date()
# print(filename)                     
# print(file_date)

logger = audit_logger()

class preprocessing_engine:
    def preprocess_file(self,ingestionpath):
        df = pd.read_csv(ingestionpath)
        df.drop_duplicates(inplace = True)
        filename = ingestionpath.split('\\')[-1]
        
        print(f"Preprocessing file - {filename}\n")
        
        datestring = filename.split('_')[1].split('.')[0]
        file_date = datetime.strptime(datestring,'%Y%m%d').date()
        system_date = date.today()
        df['ingestion_date'] = system_date
        df['file_date'] = file_date
        preprocessedPath = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed'
        updated_preprocessedPath = os.path.join(preprocessedPath, filename.split('.')[0]) + '.parquet'
        df.to_parquet(updated_preprocessedPath, index=False)
        logger.log_preprocess(filename, df.duplicated().sum())
        print(f"File preprocessed successfully - {filename}\n")
        
        # preprocessLogger.log_srcToPreprocess(updated_preprocessedPath)
       

# df = pd.read_csv(path)
# df.drop_duplicates(inplace = True)
# filename = path.split('\\')[-1]
# datestring = filename.split('_')[1].split('.')[0]
# file_date = datetime.strptime(datestring,'%Y%m%d').date()
# system_date = date.today()
# df['ingestion_date'] = system_date
# df['file_date'] = file_date    
# print(df.head(5))    