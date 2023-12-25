import pandas as pd
from sqlalchemy import create_engine
import yaml

class DataExtractor:
    @staticmethod
    def read_db_creds(file_path):
        try:
            with open(file_path, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
            return {}
        

    @staticmethod
    def init_db_engine(creds):

        # Extract credentials
        host = creds['RDS_HOST']
        user = creds['RDS_USER']
        password = creds['RDS_PASSWORD']
        database = creds['RDS_DATABASE']
        port = creds['RDS_PORT']

        try:
            engine = create_engine(
                f"postgresql://{creds['RDS_HOST']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
            return engine
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None

    @staticmethod
    def list_db_tables(engine):
        try:
            tables = engine.table_names()
            return tables
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    @staticmethod
    def read_rds_table(engine, table_name):
        try:
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return pd.DataFrame()

    @staticmethod
    def upload_to_db(engine, df, table_name):
        try:
            df.to_sql(table_name, engine, index=False, if_exists='replace')
            print(f"Data uploaded to {table_name} successfully.")
        except Exception as e:
            print(f"Error uploading data to {table_name}: {e}")

# Example usage
read_data = DataExtractor.read_db_creds('db_creds.yaml')
mrdc_engine = DataExtractor.init_db_engine(read_data)

if mrdc_engine:
    tables = DataExtractor.list_db_tables(engine)
    print(f"Available tables: {tables}")

    user_data_table = 'Data Handling Project'
    user_data_df = DataExtractor.read_rds_table(engine, user_data_table)

    if not user_data_df.empty:
        cleaned_user_data = DataCleaning.clean_user_data(user_data_df)

        connector = DatabaseConnector()
        connector.upload_to_db(engine, cleaned_user_data, 'dim_users')
