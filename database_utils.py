import yaml
import psycopg2
import sqlalchemy

class DatabaseConnector:
    def __init__(self, file_path):
        self.file_path = file_path
        self.credentials = self.read_db_creds()
        self.engine = self.init_db_engine()
        self.tables = self.list_db_tables()
        
    def read_db_creds(self):
        try:
            with open(self.file_path, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except FileNotFoundError:
            print(f"Error: File not found at {self.file_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
            return {}
        
    def init_db_engine(self):
        if not self.credentials:
            print("Error: Credentials not found.")
            return None
        try:
            # Assuming the credentials dictionary has keys RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_PORT, RDS_DATABASE
            db_url = f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
            engine = sqlalchemy.create_engine(db_url)
            return engine
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None
        
    def list_db_tables(self):
        # lists all the tables in the database so you know which tables you can extract data from
        try:
            metadata = sqlalchemy.MetaData()
            metadata.reflect(bind=self.engine)
            return metadata.tables.keys()
        
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def upload_to_db(self, df, table_name, engine):
        """
        Uploads a DataFrame to the specified table in the database.

        Parameters:
        df (pandas.DataFrame): The DataFrame to upload.
        table_name (str): The name of the table to upload the data to.
        """
        from data_cleaning import DataCleaning
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to {table_name}.")
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
           
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'eden10'
DATABASE = 'sales_data'
PORT = 5432

# Example usage:
if __name__ == "__main__":
    file_path = 'db_creds.yaml'
    db_connector = DatabaseConnector(file_path)
    print(db_connector.tables)
    





# Perform database operations using the connection
# ...