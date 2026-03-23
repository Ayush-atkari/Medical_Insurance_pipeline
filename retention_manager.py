import pandas as pd
import os
import re
import numpy as np
import pyarrow.parquet as pq
from zipfile import ZipFile
from datetime import datetime
from audit_logger import audit_logger

logger = audit_logger()

class retention_manager:
    def archive_file(self,srcfolder ,srcfile):
        print(f'Archiving source files and control files in a zip folder......\n')
        
        srcfile = srcfile.split('.')[0].split('_')[-1] # filedate is extracted for naming convention of zip file
        zip_file_name = 'archive_'+srcfile+'.zip' # combinded zip file name
        zip_file = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\retention\archive',zip_file_name)
        
        with ZipFile(zip_file,'a') as zip: # open zip file in append mode
            for file in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'): #access each file from source_files
                file_path = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files',file)
                zip.write(file_path,os.path.basename(file_path)) #append the file with basename location
                      
            for file in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files'): # access each file from control_files
                file_path = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files' , file)
                zip.write(file_path , os.path.basename(file_path)) #append the file with basename location
        print(f'Source files and Control files archived successfully in {zip_file_name}.\n')
        
        print(f'Deleting source files and control files from from respective folders......\n')
        for file in os.listdir(srcfolder): # delete file from srcfolder 
            file_path = os.path.join(srcfolder,file)
            logger.log_retention(file_path,zip_file) # log the deleted file into retention_log file
            os.remove(file_path)
            
        for file in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files'): # delete from control_files 
            file_path = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files',file)
            logger.log_retention(file_path,zip_file) # log the deleted file
            os.remove(file_path)
        print(f'Source files and control files deleted successfully.\n')
        