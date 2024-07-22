import tabula
import requests
import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy import inspect
import json
import numpy as np
import boto3
from io import StringIO
import yaml

num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
s3_url = "s3://data-handling-public/products.csv"
s3_bucket = "data-handling-public"
s3_object_key = "products.csv"


# The DataExtractor class is used to extract data from a source.
class DataExtractor:

      
    def __init__(self) -> None:
        """
        The above function is a constructor that initializes an object of a class.
        """
        pass

    def read_api_creds(self):
        """
        The function reads a YAML file containing database credentials and returns them as a dictionary.
        :return: the `creds_dict` variable, which is a dictionary containing the credentials read from
        the YAML file.
        """

        with open("api_key.yaml", 'r') as file:
            api_key_header = yaml.safe_load(file)
        return api_key_header


    def list_db_tables(self, engine):
        """
        The function `list_db_tables` retrieves a list of table names from a database using the provided
        SQLAlchemy engine.
        @param engine - The `engine` parameter is an instance of a database engine, such as
        `sqlalchemy.engine.Engine`. It is used to connect to the database and perform operations on it.
        @returns a list of table names in the database.
        """

        inspector = inspect(engine)
        self.table_list = inspector.get_table_names()
        print(self.table_list)
        return self.table_list
    

    def read_rds_table(self, engine, table_name: pd.DataFrame):
        """
        The function reads a table from a database using the provided engine and table name.
        @param engine - The `engine` parameter is the database engine object that is used to connect to
        the database. It is typically created using a library like SQLAlchemy.
        @param {pd.DataFrame} table_name - The `table_name` parameter is the name of the table in the
        database that you want to read. It should be a string value.
        @returns a pandas DataFrame object named "pd_users" if the table name exists in the database
        tables. If the table name does not exist, it prints "Invalid Table" and does not return
        anything.
        """

        con = engine
        db_tables = self.list_db_tables(engine)
        if table_name in db_tables:
            pd_users = pd.read_sql_table(table_name, con=con)
            return pd_users
        else:
            print('Invalid Table')


    def retrieve_pdf_data(self, filepath: str):
        """
        The function retrieves data from a PDF file and converts it into a pandas dataframe.
        @param {str} filepath - The `filepath` parameter is a string that represents the file path of
        the PDF file that you want to retrieve data from.
        @returns a pandas dataframe containing the data extracted from the PDF file.
        """
        
        cc_df = tabula.read_pdf(filepath, stream=False, pages='all')
        cc_df = pd.concat(cc_df)
        print("PDF converted to pandas dataframe")
        return cc_df
    

    def list_number_of_stores(self, endpoint: str, headers: dict):
        """
        The function retrieves the number of stores from a specified endpoint using an API key and
        returns the number of stores.
        @param {str} endpoint - The `endpoint` parameter is a string that represents the URL of the API
        endpoint you want to make a request to. In this case, the `endpoint` is set to
        `'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        @param {dict} headers - The `headers` parameter is a dictionary that contains the headers to be
        included in the HTTP request. In this case, it includes the `x-api-key` header with the value
        `yFBQbwXe9J3sd6zWVAMrK6lcxxr0q
        @returns the number of stores from the response JSON.
        """
        num_stores_endpoint = endpoint
        num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(num_stores_endpoint, headers=headers)
        print(f"Status Code: {response.status_code}")
        stores_list = response.json()
        return stores_list['number_stores']
    
    
    def retrieve_stores_data(self, endpoint: str, headers: dict):
        """
        The function retrieves store data from an API endpoint and returns it as a pandas DataFrame.
        @param {str} endpoint - The `endpoint` parameter is a string that represents the URL endpoint
        where the store data is retrieved from. It should be in the format of a valid URL.
        @param {dict} headers - The `headers` parameter is a dictionary that contains the headers to be
        included in the HTTP request. In this case, it includes the `x-api-key` header with a specific
        value.
        @returns a pandas DataFrame containing the store details retrieved from the specified endpoint.
        """

        curr_stores = []
        no_of_stores = self.list_number_of_stores(endpoint=num_stores_endpoint, headers=headers)
        retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
        retrieve_store_endpoint = endpoint
        for store in range(0, no_of_stores):
            response = requests.get(f"{retrieve_store_endpoint}{store}", headers=headers).json()
            curr_stores.append(pd.DataFrame(response,index=[np.NaN]))
        curr_stores_df = pd.concat(curr_stores)
        print(f'stores loaded into dataframe with {len(curr_stores_df)} rows :')
        return curr_stores_df
    

    def extract_from_s3(self, bucket: str, file_from_s3: str):
        """
        The function extracts data from a file stored in an S3 bucket and returns it as a pandas
        DataFrame.
        @param {str} bucket - The "bucket" parameter is the name of the S3 bucket from which you want to
        extract the file. It is a string that represents the name of the bucket.
        @param {str} file_from_s3 - The `file_from_s3` parameter is the name or key of the file that you
        want to extract from the S3 bucket. It is the specific file that you want to download and read
        as a pandas DataFrame.
        @returns a pandas DataFrame object.
        """

        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=bucket, Key=file_from_s3)
        s3_data = s3_object['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(s3_data))
        print('S3 file Downloaded')
        print(df)
        return df
    
    
    def extract_from_s3_json(self, bucket: str, file_from_s3: str):
        """
        The function extracts data from a JSON file stored in an S3 bucket and returns it as a pandas
        DataFrame.
        @param {str} bucket - The "bucket" parameter is the name of the S3 bucket where the JSON file is
        located.
        @param {str} file_from_s3 - The `file_from_s3` parameter is the name or key of the file that you
        want to extract from the S3 bucket. It should be a string that specifies the file path or name
        in the S3 bucket.
        @returns a pandas DataFrame object.
        """
     
        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=bucket, Key=file_from_s3)
        s3_data = s3_object['Body'].read().decode('utf-8')
        df = pd.read_json(StringIO(s3_data))
        print(df.head())
        print('S3 File Downloaded')
        return df

if __name__ == '__main__':
    db = DatabaseConnector('db_creds.yaml')
    de = DataExtractor(db)
    source_engine = db.init_db_engine()
    table_list = de.list_db_tables()
    print(table_list)
    api_key_header = de.read_api_creds()
    de.list_number_of_stores(num_stores_endpoint, api_key_header)
    de.retrieve_stores_data(retrieve_store_endpoint, api_key_header)