from datetime import date, datetime

import pandas as pd
import os
import re
import numpy as np
import pyarrow.parquet as pq
from zipfile import ZipFile

# df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\semantic\Policies_Total_ClaimAmt.parquet')
# print(df.head(5))

# df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\semantic\Cities_Total_ClaimAmt.parquet')
# print(df.head(5))
# print(df.columns.dtype)
# zip_file = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\retention\first.zip'
# for filename in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'):
#     print(filename)
    
# with ZipFile(zip_file,'a') as zip:
#     for file in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'):
#         file_path = os.path.join(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files',file)
#         zip.write(file_path, os.path.basename(file_path))

# srcfile = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files\agents_20251017.csv'
# srcfile = srcfile.split('\\')[-1].split('.')[0].split('_')[-1]
# print(srcfile)
# zip_file_name = 'archive_'+srcfile+'.zip'
# print(zip_file_name)
    
# srcfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'
# # files = os.listdir(srcfolder)
# # print(files[0])

# validfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files'
# for filename2 in os.listdir(validfolder):
#     for filename1 in os.listdir(srcfolder):
#         prefix = filename2.split('_')[0]
#         if re.match(rf"^{prefix}_(\d{{8}})\.csv$",filename1): 
#             print(filename1)
#             print(filename2)
#     if 'filename1' in locals():
#         continue
#     else:
#         print('not present')
#             # orgfilepath = os.path.join(srcfolder,filename1)
#             # validfilepath = os.path.join(validfolder,filename2)
#             # manager.is_valid(orgfilepath,validfilepath)

# def source(prefix,valid_file):
#     valid_df = pd.read_json(valid_file)
#     date_string = date.today().strftime("%Y%m%d")
#     log_src = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\ingestion_log_' + date_string + '.csv'
#     dict = {'file_name':prefix+'_'+date_string+'.csv',
#           'file_date': date_string,
#           'expected_rows': valid_df['expected_rows'][0],
#           'actual_rows': 0,
#           'status': 'Failed',
#           'ingestion_date': datetime.now()
#           }
#     df = pd.DataFrame([dict])
#     # df['file_name']=prefix+'_'+date_string+'.csv'
#     # df['file_date']= date_string
#     # df['expected_rows'] = valid_df['expected_rows'][0]
#     # df['actual_rows']=0
#     # df['status']='Failed'
#     # df['ingestion_date']= datetime.now()
#     # print(df)
#     if log_src not in os.listdir(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs'):
#             with open(log_src, 'w') as f:
#                 f.write(f"file_name,file_date,expected_rows,actual_rows,status,ingestion_date\n")
#     df.to_csv(log_src,mode='a',header = False ,index = False)
#     # with open(log_src, 'a') as f:
#     #         f.write(f"{df['file_name']},{df['file_date']},{df['expected_rows']},{df['actual_rows']},{df['status']},{df['ingestion_date']}\n")
    
def source(prefix, valid_file):

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
        
srcfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\source_files'
validfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\config\control_files'
for filename2 in os.listdir(validfolder):
    prefix = filename2.split('_')[0]
    validfilepath = os.path.join(validfolder,filename2)
    file_found = False
    for filename1 in os.listdir(srcfolder): 
        if re.match(rf"^{prefix}_(\d{{8}})\.csv$",filename1): 
            file_found = True
            orgfilepath = os.path.join(srcfolder,filename1)
    if not file_found:
        source(prefix,validfilepath)
        
log = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\logs\ingestion_log_' + '20260314' + '.csv'
df= pd.read_csv(log)

print(df)



                    