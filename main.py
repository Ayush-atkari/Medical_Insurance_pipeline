import pandas as pd
import os
import re
import numpy as np
from ingestion_manager import ingestion_manager
from preprocessing_engine import preprocessing_engine
from transformation_engine import transformation_engine
from retention_manager import retention_manager
from datetime import date, datetime
from audit_logger import audit_logger

srcfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'
validfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files'

print("Data Pipeline Execution is Starting.....\n")

## Ingestion & Validation Pipeline 
print('File Validation is in Progress....\n')

manager = ingestion_manager()
logger = audit_logger()
for filename2 in os.listdir(validfolder):
    prefix = filename2.split('_')[0]
    validfilepath = os.path.join(validfolder,filename2)
    file_found = False
    for filename1 in os.listdir(srcfolder):
        if re.match(rf"^{prefix}_(\d{{8}})\.csv$",filename1): 
            file_found = True
            orgfilepath = os.path.join(srcfolder,filename1) 
            manager.is_valid(orgfilepath,validfilepath)
    if not file_found:
        logger.log_source(prefix,validfilepath)
print('File Validation Completed.\n')

## Preprocessing(Cleaning) Pipeline
print('Preprocessing is in Progress.....\n')

engine = preprocessing_engine()
ingestion_folder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\ingestion'
for filename in os.listdir(ingestion_folder):
    ingestionpath = os.path.join(ingestion_folder, filename)
    engine.preprocess_file(ingestionpath)
    
print('Preprocessing Completed.\n')

## Transformation Pipeline ( Curated and Semantic Data is created here)  
print('Transformation is in Progress.....\n')
  
transformation = transformation_engine()
transformation.transform_files()

print('Transformation Completed.\n')

## Retention Pipeline (Archiving and Deleting files from source folder is performed)
print('Retention is in Progress.....\n')
retain = retention_manager()
files = os.listdir(srcfolder)
if not files:
    print("No files to archive.")
else:
    srcfile = files[0]
    retain.archive_file(srcfolder,srcfile)

print('Retention Completed.\n')
print('Pipeline Executed Successfully. \n')
