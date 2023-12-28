from database_utils import DatabaseConnector  # Import the DatabaseConnector class
import pandas as pd

class DatabaseExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector

        
    def read_rds_table(self, table_name):
        # Access the engine object from the DatabaseConnector instance
        engine = self.database_connector.engine
        try:
            # Running SQL query and returning table in a DataFrame
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            print(df)
            return df
        except Exception as e:
            print(f"Error reading table '{table_name}': {e}")
            return None


        # Now you can use the 'engine' object for your extraction logic
        # ...

# Example usage:
if __name__ == "__main__": # To su
    file_path = 'db_creds.yaml'
    database_connector = DatabaseConnector(file_path)
    
    # Assuming 'orders_table' is a valid table in the database
    data_extractor = DatabaseExtractor(database_connector)
    data_extractor.read_rds_table('orders_table')


   

