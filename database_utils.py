import yaml
import pandas as pd
import psycopg2
import sqlalchemy

# The `DatabaseConnector` class initializes a database connection using provided credentials.
# The `DatabaseConnector` class is a Python class that connects to a database using credentials from a
# YAML file and provides methods to read the credentials, initialize the database engine, and upload a
# cleaned dataframe to the database.

class DatabaseConnector:
    def __init__(self, file_path):
        """
        The function initializes the class instance with a YAML file.
        :param yaml_file: The `yaml_file` parameter is a string that represents the path or name of a YAML file
        """
        self.file_path = file_path
        #self.credentials = self.read_db_creds()
        #self.engine = self.init_db_engine()
        #self.tables = self.list_db_tables()
        
    def read_db_creds(self):
        """
        The function reads a YAML file containing database credentials and returns them as a dictionary.
        Returns error message in the event of no file path or reading error.
        :return: the `creds_dict` variable, which is a dictionary containing the credentials read from
        the YAML file.
        """
        try:
            with open(self.file_path, 'r') as file:
                creds_dict = yaml.safe_load(file)
                return creds_dict
        except FileNotFoundError:
            print(f"Error: File not found at {self.file_path}")
            return {}
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
            return {}
        
    def init_db_engine(self):
        """
        The function initializes a database engine using the postgresql database type and
        connects to the database using the provided credentials.
        :return: the database engine object.
        """
        try:
            # Assuming the credentials dictionary has keys RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_PORT, RDS_DATABASE
            db_url = f"postgresql://{creds_dict['RDS_USER']}:{creds_dict['RDS_PASSWORD']}@{creds_dict['RDS_HOST']}:{creds_dict['RDS_PORT']}/{creds_dict['RDS_DATABASE']}"
            engine = sqlalchemy.create_engine(db_url)
            return engine
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None
        
    # def list_db_tables(self):
        """
        Lists all the tables in the database so you know which tables you can extract data from
        """
        try:
            metadata = sqlalchemy.MetaData()
            metadata.reflect(bind=self.engine)
            return metadata.tables.keys()
        
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def upload_to_db(self, input_df, table_name, engine):
        """
        Uploads a DataFrame to the specified table in the PostgreSQL database.

        :param input_df: The input_df parameter is a pandas DataFrame that contains
        the data you want to upload to the database
        :type input_df: pd.DataFrame
        :param table_name: The table name is a string that specifies the name of the table in the
        database where the cleaned dataframe will be uploaded
        :type table_name: str
        :param connection: The "connection" parameter is the connection object that is used to connect
        to the PostgreSQL database.
        """
        try:
            input_df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Cleaned dataframe uploaded successfully to {table_name} in PostgreSQL.")
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
           

# Example usage:
if __name__ == "__main__":
    file_path = 'db_creds.yaml'
    db_connector = DatabaseConnector(file_path)
    
