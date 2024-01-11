import os
# Manually setting file path for JAVA_HOME in order to run tabula-py successfully
os.environ["JAVA_HOME"] = './miniconda3/lib/python3.11/site-packages'
import requests
from tabula import read_pdf
import boto3
import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self, data_extractor):
        self.data_extractor = data_extractor

    def clean_user_data(self, df):
        # Remove duplicate columns
        df = df.loc[:,~df.columns.duplicated()]
        
        # Remove rows with Null values
        df = df.dropna()

        # Correct the date format
        df['date_uuid'] = pd.to_datetime(df['date_uuid'], errors='coerce')

        return df
        

    def clean_card_data(self, df):
        # Drop any columns with more than 1000 null values
        df = df.dropna(thresh=1000, axis=1)
        
        # Drop rows with any null values
        df = df.dropna(how='any', axis=0)
        print(df)
        return df
    
    def called_clean_store_data(self, df):
        # Drop any columns with more than 100 null values
        df = df.dropna(thresh=1000, axis=1)
        
        # Drop rows with any null values
        df = df.dropna(how='any', axis=0)
        print(df)
        return df



        
        
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'eden10'
DATABASE = 'sales_data'
PORT = 5432

if __name__ == "__main__":
    # Initialize DatabaseConnector
    file_path = 'db_creds.yaml'
    db_connector = DatabaseConnector(file_path)

    # Data extraction and cleaning
    # ... code to create data_extractor and get df ...
    data_extractor = DataExtractor(db_connector)
    df = data_extractor.read_rds_table('orders_table')
    data_cleaning = DataCleaning(data_extractor)
   
    cleaned_df = data_cleaning.clean_user_data(df)
    # Secure local connection to 'sales_data' database and upload cleanded data to new table 'dim_users'

    engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    # Upload cleaned data to the database
    db_connector.upload_to_db(cleaned_df, 'dim_users', engine)

    # Clean card details data from PDF link
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df_card = data_extractor.retrieve_pdf_data(pdf_path)
    cleaned_card_details = data_cleaning.clean_card_data(df_card)
    db_connector.upload_to_db(cleaned_card_details, 'dim_card_details', engine)
    # API links
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    retrieve_stores_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    store_number = data_extractor.list_number_of_stores(number_of_stores_endpoint)
    number_of_stores = int(store_number['number_stores'])
    stores_df = data_extractor.retrieve_stores_data(retrieve_stores_data_endpoint, number_of_stores)
    cleaned_stores_data = data_cleaning.called_clean_store_data(stores_df)
    db_connector.upload_to_db(cleaned_stores_data,'dim_store_details', engine)






