import os
# Manually setting file path for JAVA_HOME in order to run tabula-py successfully
os.environ["JAVA_HOME"] = './miniconda3/lib/python3.11/site-packages'
import requests
import json
from tabula import read_pdf
import boto3
import pandas as pd
from database_utils import DatabaseConnector  # Import the DatabaseConnector class


class DataExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector

    def read_rds_table(self, table_name):
        # Access the engine object from the DatabaseConnector instance
        engine = self.database_connector.engine
        try:
            # Running SQL query and returning table in a DataFrame
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return None

    def retrieve_pdf_data(self, link): 
        # Use tabula to read the PDF file from the link
        try:
            # Extract data from all pages
            df_list = read_pdf(link, stream=True, pages='all')
            # Combine all DataFrames into one (if multiple pages)
            df = pd.concat(df_list, ignore_index=True)
            return df
        
        except Exception as e:
            print(f"Error retrieving PDF data: {e}")
            return None

    # Set up the header with the API key 
    headers = {
    'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }

    # Function to list the number of stores
    def list_number_of_stores(self, endpoint):
        # Set up the header with the API key 
        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
            return response.json()  # Assuming the response contains JSON data
        except requests.exceptions.HTTPError as e:
            print(f'An HTTP error occurred: {e}')
            return None
        except requests.exceptions.RequestException as e:
            print(f'A request exception occurred: {e}')
            return None

        # Function to get the details of a specific store
    def retrieve_stores_data(self, endpoint, number_of_stores):
        # Debugging: print the number of stores
        print(f"Retrieving data for {number_of_stores - 1} stores.")

        # Initialize an empty list to store the details of each store
        stores_data = []

        # Loop through the number of stores, retrieve details for each, and append to the list
        for store_number in range(1, number_of_stores + 1):  # Assuming store numbers start at 1
            try:
                # Replace placeholder with actual store number
                response = requests.get(endpoint.format(store_number=store_number), headers=self.headers)
                response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
                store_data = response.json()
                stores_data.append(store_data)
            except requests.exceptions.HTTPError as e:
                print(f'HTTP error occurred when retrieving store {store_number}: {e}')
            except requests.exceptions.RequestException as e:
                print(f'Request exception occurred when retrieving store {store_number}: {e}')
            except Exception as e:
                print(f'Unexpected error occurred when retrieving store {store_number}: {e}')

        # Convert the list of store details to a pandas DataFrame
        return pd.DataFrame(stores_data)
    
    def extract_from_s3(self, s3_address):
        # Split the S3 address into bucket name and file key
        s3_bucket, s3_key = s3_address.replace("s3://", "").split("/", 1)

        # Initialize S3 client
        s3 = boto3.client('s3')

        # Temporary file path (adjust as needed)
        temp_file_path = '/tmp/temp_file.csv'

        try:
            # Download file from S3 to temporary file
            s3.download_file(s3_bucket, s3_key, temp_file_path)

            # Read the file into a pandas DataFrame
            df = pd.read_csv(temp_file_path)

            return df
        except Exception as e:
            print(f"Error accessing S3 data: {e}")
            return None
        
    def extract_from_http_json(self, url):
        try:
            # Send a GET request to the URL
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status codes

            # Load JSON content
            data = json.loads(response.content)

            # Convert the JSON object to a pandas DataFrame
            df = pd.json_normalize(data)

            return df
        except requests.RequestException as e:
            print(f"HTTP request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    

# Example usage:
if __name__ == "__main__": 
    file_path = 'db_creds.yaml'
    db_connector = DatabaseConnector(file_path)
    data_extractor = DataExtractor(db_connector)
    '''
    # Assuming 'orders_table' is a valid table in the database
    data_extractor.read_rds_table('orders_table')
    # Using tabula-py package to return tabular data from PDF link as DataFrame
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df_card = data_extractor.retrieve_pdf_data(pdf_path)
    .info() provides summary of data types and non-null values
    
    print(df_card.info())
    print(df_card.isna().mean() * 100)

    # Variables containing the two API endpoints required for GET requests
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    retrieve_stores_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    store_number = data_extractor.list_number_of_stores(number_of_stores_endpoint)
    print(store_number)
    number_of_stores = int(store_number['number_stores'])
    # Calling retrieve data method with API endpoint as argument to return store data in DataFrame in variable
    stores_df = data_extractor.retrieve_stores_data(retrieve_stores_data_endpoint, number_of_stores)
    
    print(stores_df.head())
    print(stores_df.info())


    s3_address = 's3://data-handling-public/products.csv'
    products_df = data_extractor.extract_from_s3(s3_address)
    if products_df is not None:
        print(products_df['weight'])  # Prints the weight column of the DataFrame

    
    dates_s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_events_df = data_extractor.extract_from_http_json(dates_s3_address)
    print(date_events_df.dtypes)
    print(date_events_df.head(2))
    '''
    # Assuming 'orders_table' is a valid table in the database
    orders_df = data_extractor.read_rds_table('orders_table')
    print(orders_df['product_code'])


    # Assuming 'legacy_users' is a valid table in the database
    #users_df = data_extractor.read_rds_table('legacy_users')
    #print(users_df)
