import pandas as pd
from sqlalchemy import create_engine
import yaml

class DataExtractor:
    def __init__(self, file_path):
        self.credentials = self.read_db_creds(file_path)

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
