import boto3
from io import StringIO
import pandas as pd
import requests
import tabula
from database_utils import DatabaseConnector

class DataExtractor:
    '''
    This class can be used to extract data from different data sources.
    ''' 

    def read_rds_table(self, instance_of_DbCon_class, table_name, engine):
        '''
        This function reads a table from an RDS database using the provided SQLAlchemy engine instance.

        Args:
            instance_of_DbCon_class (DatabaseConnector): An instance of the DatabaseConnector class.
            table_name (str): The name of the table to be read from the database.
            engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the RDS database.
            
        Returns:
            pandas.DataFrame: A DataFrame containing the data from the specified table.
        '''
        if table_name == "legacy_users":
            df_legacy_users = pd.read_sql_table(table_name="legacy_users", con=engine) 
            return df_legacy_users
        elif table_name == "orders_table":
            orders_df = pd.read_sql_table(table_name="orders_table", con=engine)
            return orders_df
        elif table_name == "legacy_store_details":
            legacy_stores_df = pd.read_sql_table(table_name="legacy_store_details", con=engine)
            return legacy_stores_df

    def retrieve_pdf_data(self, link):
        '''
        This function retrieves data from a PDF located at the provided link.

        Args:
            link (str): The link to the PDF.
            
        Returns:
            pandas.DataFrame: A DataFrame containing the data extracted from the PDF.
        '''
        # use tabula to reads remote pdf into list of DataFrame using tabula
        self.link = link
        pdf_dataframe_list = tabula.read_pdf(self.link, pages='all') 
        pdf_dataframe = pd.concat(pdf_dataframe_list, ignore_index=True) # convert list into dataframe
        
        return pdf_dataframe

    def list_number_of_stores(self, num_stores_endpoint_url, header):
        '''
        This function retrieves the number of stores from an API endpoint.

        Args:
            num_stores_endpoint_url (str): The URL of the API endpoint to get the number of stores.
            header (dict): The header containing the API key.
            
        Returns:
            int: The number of stores retrieved from the API.
        '''
        response = requests.get(num_stores_endpoint_url, headers=header)
        number_of_stores = response.json().get("number_stores", 0)
        
        return number_of_stores 
    
    def retrieve_stores_data(self, store_endpoint_template, number_of_stores, header):
        '''
        This function retrieves data for all stores from an API and saves them in a DataFrame.

        Args:
            store_endpoint_template (str): The template URL for retrieving store data.
            number_of_stores (int): The total number of stores.
            header (dict): The header containing necessary authentication details.
            
        Returns:
            pandas.DataFrame: A DataFrame containing the data for all stores.
        '''
        stores_list = []
        store_num = 1
      
        for store_num in range(0, number_of_stores):
            full_endpoint = store_endpoint_template + str(store_num)
            response = requests.get(full_endpoint, headers=header)
            store_data = response.json()
            stores_list.append(store_data)
            store_num += 1

        stores_df = pd.DataFrame(stores_list)
    
        return stores_df

    def extract_from_s3(self, s3_address):
        '''
        This function extracts data from an S3 bucket based on the provided address.

        Args:
            s3_address (str): The S3 address specifying the bucket and object key.
            
        Returns:
            pandas.DataFrame: A DataFrame containing the data extracted from the S3 bucket.
        '''
        # check logged into aws cli 'aws configure list'
        s3 = boto3.client('s3')
        # split address into bucket name and object key
        bucket_name, object_key = s3_address.replace("s3://", "").split("/", 1)

        # check file type based on extension
        _, file_extension = object_key.rsplit('.', 1) # underscore is throwaway variable
        
        # Download the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        
        # Convert CSV content to DataFrame based on filetype
        if file_extension.lower() == 'json':
            df = pd.read_json(content)
        elif file_extension.lower() == 'csv':
            df = pd.read_csv(StringIO(content))

        return df
    

    """
    def read_rds_table(self, instance_of_DbCon_class, table_name, engine):
        try:
            df = pd.read_sql_table(table_name=table_name, con=engine)
            if df.empty:
                print(f"Warning: The table '{table_name}' is empty.")
        return df
    except Exception as e:
        print(f"Error reading table '{table_name}': {str(e)}")
        return None
    """

if __name__ == "__main__":
    file_path = 'db_creds.yaml'
    db_con = DatabaseConnector()
    de = DataExtractor()

    print(de.read_rds_table(db_con,'orders_table',engine))
