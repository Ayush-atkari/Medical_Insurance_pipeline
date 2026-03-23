import pandas as pd
import os
import re
import numpy as np
import schedule
from ingestion_manager import ingestion_manager
from preprocessing_engine import preprocessing_engine
from transformation_engine import transformation_engine
from retention_manager import retention_manager
from datetime import date, datetime
from audit_logger import audit_logger

## Main Pipeline funtion which will be called by the scheduler everyday
def pipeline():
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
            if re.match(rf"^{prefix}_(\d{{8}})\.csv$",filename1): # ensure the source file is valid w.r.t valid file
                file_found = True # for checking if file is available in src and valid both
                orgfilepath = os.path.join(srcfolder,filename1) 
                manager.is_valid(orgfilepath,validfilepath)
        if not file_found: # if file data is available in control_files but not in source_files then log the file. 
            logger.log_source(prefix,validfilepath)


    files = os.listdir(srcfolder) 
    if not files: # if no files exists in source_files folder then no need of processing further files.
        print('No files found in source folder for validation.Please add source files and re-run the pipeline.\n')
    else: # Continue the further processing
        print('File Validation Completed.\n')
    ## Preprocessing(Cleaning) Pipeline
        print('Preprocessing is in Progress.....\n')
        
        engine = preprocessing_engine()
        ingestion_folder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\ingestion'
        for filename in os.listdir(ingestion_folder): # send each validated files from ingestion folder to the preprocessing engine() one by one
            ingestionpath = os.path.join(ingestion_folder, filename)
            engine.preprocess_file(ingestionpath)
            
        print('Preprocessing Completed.\n')

        ## Transformation Pipeline ( Curated and Semantic Data is created here)  
        print('Transformation is in Progress.....\n')
        # transform_files() is called
        transformation = transformation_engine()
        transformation.transform_files()

        print('Transformation Completed.\n')

        ## Retention Pipeline (Archiving and Deleting files from source folder is performed)
        print('Retention is in Progress.....\n')
        retain = retention_manager()
        srcfile = files[0] # one filename is sent as an argument to get the date of the file for zip folder name
        retain.archive_file(srcfolder,srcfile) # archive_file() is called with srcfolder path and one filename
        print('Retention Completed.\n')
        print('Pipeline Executed Successfully. \n')



## schedule the pipeline execution everyday at 10:00 
schedule.every().day.at("10:00").do(pipeline)

while True:
    schedule.run_pending()