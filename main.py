import numpy as np
import matplotlib.pyplot as plt
import yaml
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text

from database_utils import DatabaseConnector
from data_cleaning import DatabaseCleaning
from data_extraction import DataExtractor

''' This is the script where I will use the three different classes (DatabaseConnector,
DataExtractor and DatabaseCleaning) to retrive data from a variety of sources, clean 
the data and upload to the sales_data database in the relational database management 
system PostgreSQL. Finally, it will run two sql scripts which will create the database schema 
and run business queries on it, visualising one query with a piechart.'''

def user_data():
    '''
    This function retrieves, cleans, and uploads user data from an AWS RDS database to a new table in the sales_data database.

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing user data, which has also been uploaded to the "dim_users" table.
    '''
    # create instance of DataExtractor class 
    extract_rds_data = DataExtractor()
    # Get the users table name
    legacy_users_table_name = get_table_names[1]
    # Use it to read in/retrieve the data from the RDS table, which returns a dataframe
    users_df = extract_rds_data.read_rds_table(database_connector, legacy_users_table_name, engine)

    # Create an instance of DatabaseCleaning class
    clean_user = DatabaseCleaning()
    # use clean_user_data() method to clean the data
    clean_user_df = clean_user.clean_user_data(users_df)

    # Upload to a new table called dim_users in SQAlchemy sales_data database.
    database_connector.upload_to_db(clean_user_df, "dim_users", 'db_local_creds.yaml')
    return clean_user_df

def card_data():
    '''
    This function retrieves, cleans, and uploads card data from a PDF document in an AWS S3 bucket into a new table in the sales_data database.

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing card data, which has also been uploaded to the "dim_card_details" table.
    '''
    # Create instance of DBConnector class
    extract_pdf_data = DataExtractor()
    # takes in uncleaned df as arg, sets it to cleaned_df variable
    card_details_df = extract_pdf_data.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    
    # Create Cleaning instanct
    card_cleaning = DatabaseCleaning()
    # Use clean_card_data method to clean df
    clean_card_df = card_cleaning.clean_card_data(card_details_df)
    
    # upload to a new table called dim_card_details in SQAlchemy sales_data database
    database_connector.upload_to_db(clean_card_df, "dim_card_details", 'db_local_creds.yaml')
    
    return clean_card_df

def stores_data():
    '''
    This function retrieves, cleans, and uploads stores data using an API into a new table in the sales_data database.

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing store data, which has also been uploaded to the "dim_store_details" table.
    '''
    retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/" # number of stores will be added onto the end of this
    # Use retrieve_stores_data method to return the df
    store_info_df = api_retrieval.retrieve_stores_data(retrieve_store_endpoint, total_stores, api_header_details)

    # clean stores_df
    store_cleaning = DatabaseCleaning()
    clean_store_df = store_cleaning.clean_store_data(store_info_df)

    # Upload to a new table called dim_store_details in SQAlchemy sales_data database
    database_connector.upload_to_db(clean_store_df, "dim_store_details", 'db_local_creds.yaml')

    return clean_store_df

def product_data():
    '''
    This function retrieves, cleans, and uploads product data from a csv file in an s3 bucket into a new table in the sales_data database.

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing product data, which has also been uploaded to the "dim_products" table.
    '''
    # S3 URI:
    s3_products_address = database_connector.read_db_creds('s3_url.yaml')

    # use the extract_from_s3 method to input the s3 address and return a dataframe
    get_product_details = DataExtractor()
    precleaned_product_df = get_product_details.extract_from_s3(s3_products_address)
    
    # Use convert_product_weights method to convert weights column to same unit (kg)
    clean_product_data = DatabaseCleaning()
    product_weight_kg_df = clean_product_data.convert_product_weights(precleaned_product_df)

    # Clean the rest of the dataframe
    cleaned_product_df = clean_product_data.clean_products_data(product_weight_kg_df)

    # upload to sales_data database using upload_to_db method in a table named dim_products
    database_connector.upload_to_db(cleaned_product_df, "dim_products", 'db_local_creds.yaml')

    return cleaned_product_df

def orders_data():
    '''
    This function retrieves, cleans, and uploads orders data from an AWS RDS into a new table in the sales_data database.

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing orders data, which has also been uploaded to the "orders_table" table.
    '''
    # get table name associated with orders 
    orders_table_name = get_table_names[2]

    # create instance of DataExtractor class 
    extract_rds_data = DataExtractor()
    rds_orders_df = extract_rds_data.read_rds_table(database_connector, orders_table_name, engine)

    # use it to clean the df and return clean df
    clean_orders_df = DatabaseCleaning()
    orders_df = clean_orders_df.clean_orders_data(rds_orders_df)

    # Upload to sales_data database using upload_to_db method in a table named orders_table
    database_connector.upload_to_db(orders_df, "orders_table", 'db_local_creds.yaml')

    return orders_df

def date_data():
    '''
    This function retrieves, cleans, and uploads orders data from a JSON file in s3 bucket into a new table in the sales_data database.

    Returns:
        pandas.DataFrame: A cleaned DataFrame containing date data, which has also been uploaded to the "dim_date_times" table.
    '''
    # create a DataExtractor instance
    extract_json_data = DataExtractor()

    # Use read_db_creds method to get link to json object
    json_s3_address = database_connector.read_db_creds('json_s3_url.yaml')

    # use extract_from_s3 method to get a dataframe
    date_time_details_df = extract_json_data.extract_from_s3(json_s3_address)

    # Clean the data using clean_date_data 
    date_cleaning = DatabaseCleaning()
    clean_date_df = date_cleaning.clean_date_data(date_time_details_df)

    # Upload to sales_data database using upload_to_db method in a table named dim_date_times
    database_connector.upload_to_db(clean_date_df, "dim_date_times", 'db_local_creds.yaml')

    return clean_date_df

def execute_schema_sql_file(creds, file_path):
    '''
    This function takes the SQL script from the given file to create the database schema.

    Args:
        creds (str): PostgreSQL connection string.
        file_path (str): Path to the SQL script file.

    Returns:
        None
    '''
    with open(file_path, 'r') as sql_file:
        sql_script = sql_file.read()
    conn = psycopg2.connect(creds)

    try:
        # Create a cursor object
        cursor = conn.cursor()
        # Execute the SQL script
        cursor.execute(sql_script)
        # Commit the changes
        conn.commit()
        print("SQL script 'create_schema' executed successfully.")

    except Exception as e:
        print(f"Error executing 'create_schema' SQL script: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def execute_query_sql_file(creds, file_path):
    '''
    This function takes the SQL script from the given file and prints the results of the queries.

    Args:
        creds (str): PostgreSQL connection string.
        file_path (str): Path to the SQL script file.

    Returns:
        None
    
    '''
    # Open the SQL file to read mode
    with open(file_path, 'r') as sql_file:
        sql_script = sql_file.read()
    
    # Set up connection
    conn = psycopg2.connect(creds)

    try:
        # Create a cursor object
        cursor = conn.cursor()
        # Split SQL script into individual statements
        sql_commands = sql_script.split(';')
        print("QUERIES: Q1. Returns the number of stores the business has in each country. Q2. Returns the top five locations which have the most stores. Q3. Returns the top 5 months that produced the largest number of sales. Q4. Returns the  amount made [0], number of products sold [1] for online and offline [3] purchases. Q5. Returns for each store type [0] the total sales [1] and percentage of sales [2] which came through. (Visualised with a pie chart too!) Q6. Returns the top 5 months [2] and years [1] in history that made the most amount is sales [0]. Q7. Returns staff headcount [0] by country [1]. Q8. Returns which 5 German [2] store types [1] have the highest total_sales [0]. Q9. The average time taken between each sale [1] grouped by year [0].")
        query_number = 1

        for command in sql_commands:
            # Skip empty statements
            if not command.strip():
                continue

            try:
                # Execute the SQL command
                cursor.execute(command)
                # Fetch results for sql statements regardless of case
                if command.strip().upper().startswith("SELECT") or command.strip().upper().startswith("WITH"):
                    data = cursor.fetchall()
                    print(f"Query {query_number} result:")
                    print(data)
                    # Increase Q number
                    query_number += 1

            except Exception as e:
                print(f"Error executing SQL command: {e}")

        # Commit the changes
        conn.commit()

        print("SQL script 'business_queries' executed successfully.")

    except Exception as e:
        print(f"Error executing 'business_queries' SQL script: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

def storetype_sales_piechart():
    '''
    This function generates a pie chart using Matplotlib to visualize the percentage of sales that come through each type of store in the database.
    Generate a pie chart to visualize the percentage of sales through each type of store.

    Args:
        None

    Returns:
        None
    '''
    fig = plt.figure()
    # Add figure axes
    ax = fig.add_axes([0,0,1,1])
    # set aspect ratio for it to be circular
    ax.axis('equal')

    # Define store types
    store_type = ['Local', 'Web Portal', 'Super Store', 'Mall Kiosk', 'Outlet']
    percentage_total = np.array([44.557729, 22.357841, 15.853934, 9.048969, 8.181527])
    
    ax.pie(percentage_total, labels=store_type, autopct='%1.2f%%') # autopct='%1.2f%% adds percentage labels with two decimal places.
    ax.set_title('Percentage of Sales by Store Type')

    plt.show()


if __name__ == "__main__":
    ### 1. Creating a connection to the AWS database

    # Create an instance of DatabaseConnector class
    database_connector = DatabaseConnector()
    # Use read_db_creds mehtod to return yaml credentials in useable format
    yaml_creds = database_connector.read_db_creds('db_creds.yaml')
    # Use these credentials to create an SQLAlchemy database engire to connect to the RDS
    engine = database_connector.init_db_engine('db_creds.yaml')
    # List the table names that are in the AWS RDS database
    get_table_names = database_connector.list_db_tables('db_creds.yaml')
    #print(get_table_names) # ['legacy_store_details', 'legacy_users', 'orders_table']

    ### 2. Retrive, clean and upload the user data from an AWS database in the cloud.
    user_data()
    ### 3. Retrive, clean and upload the card data from a PDF document in an AWS S3 bucket
    card_data()
    ### 4. Extract, clean and upload store details using an API
    '''The store data can be retrieved through the use of an API.
    The API has two GET methods. One will return the number of stores in the business and the other to retrieve a store given a store number.

    The two endpoints for the API are as follows:
    Retrieve a store: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}
    Return the number of stores: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'''
    # Use read_db_creds method to read yaml file with api key
    api_header_details = database_connector.read_db_creds('api_key.yaml')
    num_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    api_retrieval = DataExtractor()
    # Use the list_number_of_stores method to get the number of total stores
    total_stores = api_retrieval.list_number_of_stores(num_of_stores_endpoint, api_header_details)
    #print(total_stores) # 451
    stores_data()
    ### 5. Extract, edit, clean and upload data from product details from csv file in s3 bucket
    product_data()
    ### 6. Retrieve orders table from AWS RDS
    orders_data()
    ### 7. Retrieve, clean and upload date events data from JSON file in s3 bucket.
    date_data()   

    ### 8. Create the Database Schema
    # This will be run using the create_schema.sql file and includes casting column datatypes, adding descriptive columns, assigning primary and foreign keys.

    # Create connection to postgresql
    postgres_creds = database_connector.read_db_creds('db_local_creds.yaml')
    connection_str = f"host={postgres_creds['HOST']} dbname={postgres_creds['DATABASE']} user={postgres_creds['USER']} password={postgres_creds['PASSWORD']}"

    # Specify SQL file
    schema_sql_file_path = 'sql_files/essential_queries/create_schema.sql'
    # Run function to create the schema
    execute_schema_sql_file(connection_str, schema_sql_file_path)

    ### 9. Query the Database  
    # Answering business questions about sales 

    # Specify SQL file
    query_sql_file_path = 'sql_files/essential_queries/business_queries_copy.sql'
    # Run function to query the database
    execute_query_sql_file(connection_str, query_sql_file_path)

    # Here's a visual representation of the result of queary 5: What percentage of sales come through each type of store?
    storetype_sales_piechart()
