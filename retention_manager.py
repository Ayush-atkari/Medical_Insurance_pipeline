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
        print(f'Archiving source files in a zip folder......\n')
        
        srcfile = srcfile.split('.')[0].split('_')[-1]
        zip_file_name = 'archive_'+srcfile+'.zip'
        zip_file = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\retention\archive',zip_file_name)
        
        with ZipFile(zip_file,'a') as zip:
            for file in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'):
                file_path = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files',file)
                zip.write(file_path, os.path.basename(file_path))
        print(f'Source files archived successfully in {zip_file_name}.\n')
        
        print(f'Deleting source files from source folder......\n')
        for file in os.listdir(srcfolder):
            file_path = os.path.join(srcfolder,file)
            logger.log_retention(file_path,zip_file)
            os.remove(file_path)
        print(f'Source files deleted successfully from source folder.\n')
        