import pandas as pd
import os
import re
import numpy as np
import pyarrow.parquet as pq



class transformation_engine:
    def transform_files(self):
        curatedPath = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\curated\claims_enriched.parquet'
        ppfolder = r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed'
        for file in os.listdir(ppfolder): #extract each file from preprocessed folder
            prefix = file.split('_')[0] # extract file prefix - claims , customers , payments , policies for correct assignment in df.
            file_path = os.path.join(ppfolder,file)
            if prefix == 'claims':
                claims_df = pd.read_parquet(file_path)
            elif prefix == 'customers':
                customers_df = pd.read_parquet(file_path)
            elif prefix == 'payments':
                payments_df = pd.read_parquet(file_path)
            elif prefix == 'policies':
                policies_df = pd.read_parquet(file_path)
            else:
                continue
    
        claims_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True) # drop the extra columns which were added in preprocessing step
        customers_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)
        payments_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)
        policies_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)
        
        claimpoliciesdf = claims_df.merge(policies_df , how='left',on = 'Policy_ID') # merge claims and policies on common column - Policy_ID
        claimpoliciescustomers=claimpoliciesdf.merge(customers_df, on="Customer_ID", how="left") # merge the result from above step with customers on customer_ID
        claimpoliciescustomerspayments = claimpoliciescustomers.merge(payments_df, on="Policy_ID", how="left", suffixes=("", "_payments")) # merge the result from above step with payments on Policy_ID
        claimpoliciescustomerspayments.drop_duplicates(inplace = True) # drop duplicates if any
        
        # if there already exists the curated file then append the result from the df to the existing file else store df as curated file directly
        if os.path.exists(curatedPath):
            existing_df = pd.read_parquet(curatedPath)
            result = pd.concat([claimpoliciescustomerspayments, existing_df]).drop_duplicates().reset_index(drop=True)
            result.to_parquet(curatedPath, index = False)
        else:
            claimpoliciescustomerspayments.to_parquet(curatedPath, index = False)
        
        print(f'claims_enriched.parquet file saved in curated folder.\n')
        
        df = pd.read_parquet(curatedPath) 
        
        # create 1st semantic file with help of curated file.   
        policy_filter = df.groupby('Policy_Type').agg(
            TotalClaimAmount = ('Claim_Amount','sum'),
            Claims_Count = ('Claim_ID','count')
        )
        # create 2nd semantic file 
        city_filter = df.groupby('City').agg(
            Avg_Claim_Amount = ('Claim_Amount','mean'),
            Claims_Count = ('Customer_ID','count')
        )
        
        # store 1st semantic file
        policy_filter.to_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\semantic\Policies_Total_ClaimAmt.parquet')
        print(f'policies_total_claimamt.parquet file saved in semantic folder.\n')
        
        # store 2nd semantic file
        city_filter.to_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\semantic\Cities_Total_ClaimAmt.parquet')
        print(f'cities_total_claimamt.parquet file saved in semantic folder.\n')
        
# agent_df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed\agents_20251017.parquet')
# print(agent_df.head(5))
# claims_df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed\claims_20251017.parquet')
# customers_df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed\customers_20251017.parquet')
# payments_df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed\payments_20251017.parquet')
# policies_df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\preprocessed\policies_20251017.parquet')
# # print(f'Claims:\n {claims_df.head(5)}\n')
# # print(f'Customers:\n {customers_df.head(5)}\n')
# # print(f'Payments:\n {payments_df.head(5)}\n')
# # print(f'Policies:\n {policies_df.head(5)}\n')


# claims_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)
# customers_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)
# payments_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)
# policies_df.drop(['ingestion_date','file_date'],axis = 1,inplace = True)

# claimpoliciesdf = claims_df.merge(policies_df , how='left',on = 'Policy_ID')
# # print(claimpoliciesdf.head(5))

# claimpoliciescustomers=claimpoliciesdf.merge(customers_df, on="Customer_ID", how="left")
# # print(claimpoliciescustomers.head(5))

# claimpoliciescustomerspayments = claimpoliciescustomers.merge(payments_df, on="Policy_ID", how="left", suffixes=("", "_payments"))
# print(claimpoliciescustomerspayments.head(5))
# print(claimpoliciescustomerspayments.duplicated().sum())
# print(claimpoliciescustomerspayments.shape[0])
# claimpoliciescustomerspayments.drop_duplicates(inplace = True)
# result = pd.concat([claimpoliciescustomerspayments, claimpoliciescustomerspayments]).drop_duplicates().reset_index(drop=True)
# print(result.head(5))
# print(result.shape[0])
# df = pd.read_parquet(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\curated\claims_enriched.parquet')
# print(df.head(5))   
# print(df.shape[0])  
# print(df.columns)  

# filtered = df.groupby('Policy_Type').agg(
#     TotalClaimAmount = ('Claim_Amount','sum'),
#     TotalClaims = ('Claim_ID','count')
# )
# print(filtered)

#filtered.to_csv(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\semantic\claims_summary.csv',index = True)

# df2 = pd.read_csv(r'C:\Users\ayush\OneDrive\Desktop\python_intellibi\devInsureDataPipeline\data\semantic\claims_summary.csv')
# print(df2.head(5))