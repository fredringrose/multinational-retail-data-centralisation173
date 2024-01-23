import os
# Manually setting file path for JAVA_HOME in order to run tabula-py successfully
os.environ["JAVA_HOME"] = './miniconda3/lib/python3.11/site-packages'
import requests
import json
from tabula import read_pdf
import boto3
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype
import re
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

        import pandas as pd

        # Casting the columns to the correct data types

        # Ensure 'first_name' and 'last_name' are stored as strings (VARCHAR)
        df['first_name'] = df['first_name'].str.slice(0, 255).astype('string')
        df['last_name'] = df['last_name'].str.slice(0, 255).astype('string')

        # Convert 'date_of_birth' and 'join_date' from text to datetime
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        # Ensure 'country_code' is stored as string (VARCHAR)
        df['country_code'] = df['country_code'].astype('string')

        # Ensure 'user_uuid' is stored as string (UUID)
        df['user_uuid'] = df['user_uuid'].astype('string')

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
    
    def convert_product_weights(self, products_df):
        def convert_to_kg(weight):
            # Use regular expression to separate number and unit
            match = re.match(r'(\d+\.?\d*)\s*(\w+)', weight)
            if match:
                value, unit = match.groups()
                value = float(value)

                # Convert to kg based on the unit
                if unit in ['g', 'ml']:  # Assuming ml is treated the same as g
                    return value / 1000
                elif unit == 'kg':
                    return value
                # Add more unit conversions if needed
            return None

        # Apply the conversion to the weight column
        products_df['weight'] = products_df['weight'].apply(lambda x: convert_to_kg(x) if pd.notna(x) else x)

        return products_df
    
    def clean_products_data(self, df):
        # Remove the first column (assuming it's a duplicate index)
        cleaned_df = df.iloc[:, 1:]

        # Remove rows with null or missing values
        cleaned_df = cleaned_df.dropna()

        return cleaned_df
    
    def clean_orders_data(self, df):
        # Check and remove the first two unnamed columns if they exist
        df = df.iloc[:, 2:]

        # Remove 'first_name' and 'last_name' columns if they exist
        columns_to_remove = ['first_name', 'last_name']
        df = df.drop(columns=columns_to_remove, errors='ignore')

        ''' Casting the columns to the correct data types '''
        # Check if 'date_uuid' and 'user_uuid' columns are strings and if so, cast them to string (UUIDs are stored as strings in pandas)
        if is_string_dtype(df['date_uuid']):
            df['date_uuid'] = df['date_uuid'].astype('string')
        if is_string_dtype(df['user_uuid']):
            df['user_uuid'] = df['user_uuid'].astype('string')

        # Check if 'card_number', 'store_code', 'product_code' are strings and if so, cast them to string
        if is_string_dtype(df['card_number']):
            df['card_number'] = df['card_number'].str.slice(0, 16).astype('string')

        if is_string_dtype(df['store_code']):
            df['store_code'] = df['store_code'].str.slice(0, 12).astype('string')

        if is_string_dtype(df['product_code']):
            df['product_code'] = df['product_code'].str.slice(0, 11).astype('string')

        # Check if 'product_quantity' is a numeric type and if so, cast to int (smallint)
        if is_numeric_dtype(df['product_quantity']):
            df['product_quantity'] = df['product_quantity'].astype('int16')


        return df
    
    def clean_events_data(self, df):
        # Regular expression for the correct timestamp format
        timestamp_regex = r'^\d{2}:\d{2}:\d{2}$'

        # Function to check if all values in a column match the timestamp format
        def all_values_are_valid_timestamps(column):
            return all(column.apply(lambda x: bool(re.match(timestamp_regex, str(x)))))

        # List to hold columns to drop
        columns_to_drop = []

        # Check each column and mark it for dropping if it contains invalid data
        for col in df.columns:
            if not all_values_are_valid_timestamps(df[col]):
                columns_to_drop.append(col)

        # Drop the marked columns
        df = df.drop(columns=columns_to_drop)

        return df

'''

In this modified `clean_events_data` method:

- The `all_values_are_valid_timestamps` function is used to check if all values in a given column match the 'HH:MM:SS' timestamp format.
- The method iterates over each column in the DataFrame `df`. If any column contains values that do not match the timestamp format or are null/NaN, that column is added to a list of columns to be dropped.
- The method then drops all the columns in the `columns_to_drop` list from the DataFrame.
- Finally, the cleaned DataFrame, with only the columns containing valid timestamps, is returned.

This approach ensures that the final DataFrame only includes columns where all values are correctly formatted timestamps, removing any columns with incorrect or null values.

'''
# Example usage
# Assuming you have a DataFrame 'events_df' with timestamp data
# data_cleaning = DataCleaning()



        
        
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
    users_df = data_extractor.read_rds_table('legacy_users')
    # Create instance of DataCleaning class
    data_cleaning = DataCleaning(data_extractor)
    # Secure local connection to 'sales_data' database
    engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    '''
    # 1. User details #
    cleaned_users_df = data_cleaning.clean_user_data(users_df)
    # Upload cleaned data to the database in new table dim_users
    db_connector.upload_to_db(cleaned_users_df, 'dim_users', engine)

    # 2. Card details #
    # Clean card details data from PDF link
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df_card = data_extractor.retrieve_pdf_data(pdf_path)
    cleaned_card_details = data_cleaning.clean_card_data(df_card)
    db_connector.upload_to_db(cleaned_card_details, 'dim_card_details', engine)
    # API links
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    retrieve_stores_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    # 3. Store details #
    store_number = data_extractor.list_number_of_stores(number_of_stores_endpoint)
    number_of_stores = int(store_number['number_stores'])
    stores_df = data_extractor.retrieve_stores_data(retrieve_stores_data_endpoint, number_of_stores)
    cleaned_stores_data = data_cleaning.called_clean_store_data(stores_df)
    db_connector.upload_to_db(cleaned_stores_data,'dim_store_details', engine)

    # 4. Product details #
    products_s3_address = 's3://data-handling-public/products.csv'
    products_df = data_extractor.extract_from_s3(products_s3_address)
    convert_product_details = data_cleaning.convert_product_weights(products_df)
    cleaned_product_details = data_cleaning.clean_products_data(convert_product_details)
    db_connector.upload_to_db(cleaned_product_details,'dim_products', engine)

    # 5. Master orders table #
    orders_df = data_extractor.read_rds_table('orders_table')
    cleaned_orders_df = data_cleaning.clean_orders_data(orders_df)
    db_connector.upload_to_db(cleaned_product_details,'orders_table', engine)
    
    # 6. Date events details #
    dates_s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_events_df = data_extractor.extract_from_http_json(dates_s3_address)
    cleaned_date_events_df = data_cleaning.clean_events_data(date_events_df)
    transposed_cleaned_df = cleaned_date_events_df.transpose()
    db_connector.upload_to_db(transposed_cleaned_df, 'dim_date_times', engine)

    '''
    ## Testing star-based schema ##
    # 1. User details #
    cleaned_users_df = data_cleaning.clean_user_data(users_df)
    print(cleaned_users_df['join_date'])



