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
        # lists all the tables in the database using SQLAlchemy's MetaData container object so you know which tables you can extract data from
        try:
            metadata = sqlalchemy.MetaData()
            metadata.reflect(bind=self.engine)
            return metadata.tables.keys()
        
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []


# Example usage:
file_path = 'db_creds.yaml'
database_connector = DatabaseConnector(file_path)
print(database_connector.tables)



# Perform database operations using the connection
# ...