import yaml
import psycopg2
import sqlalchemy

class DataBaseConnector:
    def __init__(self, file_path):
        self.credentials = self.read_db_creds(file_path)

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
        
def init_db_engine(self):
        if not self.credentials:
            print("Error: Credentials not found.")
            return None

        try:
            # Assuming the credentials dictionary has keys RDS_USER, RDS_PASSWORD, RDS_HOST, RDS_PORT, RDS_DATABASE
            db_url = f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
            engine = create_engine(db_url)
            return engine
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            return None


# Example usage:
file_path = 'db_creds.yaml'
database_connector = DataBaseConnector(file_path)
credentials = database_connector.read_db_creds(file_path)
print(credentials)
engine = database_connector.init_db_engine()



# Perform database operations using the connection
# ...