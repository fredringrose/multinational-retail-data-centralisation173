from sqlalchemy import create_engine, text, inspect
import yaml

class DatabaseConnector:
    '''
    This class can be used to connect to and upload to the sales_data database. 

    ''' 

    def read_db_creds(self, file):
        '''
        This function reads the contents of a YAML file and returns a dictionary.

        Args:
            file (str): Path to the YAML file containing credentials.

        Returns:
            dict: python dictionary containing the yaml file contents.
        '''
        # Open YAML file in read mode
        with open(file, 'r') as stream: 
            # Use PyYAML's safe_load to parse the YAML content into a Python dictionary
            data_loaded = yaml.safe_load(stream)
    
        return data_loaded
    
    def init_db_engine(self, file):
        '''
        This function uses the yaml credentials dictionary to initialise and return a SQAlchemy database engine.

        Args:
            file (str): Path to the YAML file containing the database credentials.

        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy engine connected to the specified database.
        '''
        # Read database credentials from the specified YAML file
        dict_yaml_func = self.read_db_creds(file)

        # Create a SQLAlchemy engine using the database credentials
        engine = create_engine(f"postgresql+psycopg2://{dict_yaml_func['USER']}:{dict_yaml_func['PASSWORD']}@{dict_yaml_func['HOST']}:{dict_yaml_func['PORT']}/{dict_yaml_func['DATABASE']}")
        # Connect to the database using the engine
        engine.connect()
        return engine

    def list_db_tables(self, file):
        '''
        This function lists all tables in the connected database.

        Args:
            file (str): Path to the YAML file containing the database credentials.

        Returns:
            list: A list of table names in the 'public' schema.
        '''
        # Create the database engine
        engine = self.init_db_engine(file) 
        # Inspect the structure of the database
        inspector = inspect(engine)
        # Get a list of table names in the 'public' schema
        table_names = inspector.get_table_names() 

        # Connect to the database and use an SQL query to retrieve table names
        with engine.connect() as connection:
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
            list_of_tables = []
            for table in result:
                list_of_tables.append(table)
        unpacked_tuples_list = [table[0] for table in list_of_tables]
        
        return unpacked_tuples_list
    
    def upload_to_db(self, input_df, table_name, file):
        '''
        This function uploads a Pandas DataFrame to the specified table in the connected PostgreSQL database. 

        Args:
            input_df (pandas.DataFrame): The DataFrame to be uploaded to the database.
            table_name (str): The name of the table to which the DataFrame should be uploaded.
            file (str): Path to the YAML file containing the database credentials.
        
        '''  
        eng_con = self.init_db_engine(file)
        # creates table
        input_df.to_sql(table_name, eng_con, if_exists='replace', index=False)  

