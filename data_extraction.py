import pandas as pd
from sqlalchemy import create_engine
import yaml
from database_utils import DatabaseConnector

class DatabaseExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector

    def read_rds_table(self, table_name):
        if table_name not in self.database_connector.tables:
            print(f"Error: Table '{table_name}' not found in the database.")
            return None

        try:
            # Running SQL query and returning table in a DataFrame
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.database_connector.engine)
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return None

# Example usage:
file_path = 'db_creds.yaml'
database_connector = DatabaseConnector(file_path)
database_extractor = DatabaseExtractor(database_connector)

# Assuming 'orders_table' is a valid table in the database
orders_data = database_extractor.read_rds_table('orders_table')

# Use the 'orders_data' DataFrame as needed
