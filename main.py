# 6 pipes lines, each pipeline will look like a function with a bunch of method calls from other python files
# user data create instancee of connector class and feed dbcredsyaml file the create instanc of cleaning class then extractor class
#first user data pipeline, when extract data from RDS us db_creds, push to postgres use pgadmin4_creds


from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



if __name__ == '__main__':
 

 
 def orders_run():
      """
      The function `orders_run()` connects to a database, extracts data from a specific table, cleans the
      data, and uploads the cleaned data to a different table in the database.
      """
      db_local = DatabaseConnector('db_local_creds.yaml')
      local_engine = db_local.init_db_engine()
      db2 = DatabaseConnector('db_creds.yaml')
      de2 = DataExtractor()
      table_list = de2.list_db_tables(engine=db2.init_db_engine())
      orders_raw = de2.read_rds_table(engine=db2.init_db_engine(), table_name=table_list[2])
      orders_cleaned_init = DataCleaning(orders_table=orders_raw)
      cleaned_orders = orders_cleaned_init.clean_orders_table()
      db2.upload_to_db(cleaned_dataframe=cleaned_orders, table_name='orders_table', connection=local_engine)
 
 orders_run()

 db = DatabaseConnector('db_creds.yaml')
 de = DataExtractor()
 eng = db.init_db_engine()
 db_local = DatabaseConnector('db_creds_local.yaml')
 local_engine = db_local.init_db_engine()
 pdf_file = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
 num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
 retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
 s3_url = "s3://data-handling-public/products.csv"
 s3_bucket = "data-handling-public"
 s3_object_key = "products.csv"
 s3_json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
 s3_json_bucket = "data-handling-public"
 s3_json_object_key = "date_details.json"   
 
 def users_run():
      """
      The function `users_run()` retrieves data from a database table, cleans the data, and uploads the
      cleaned data to another table in the database.
      """

      table_list = de.list_db_tables(engine=eng)
      users_raw = de.read_rds_table(engine=eng, table_name=table_list[1])
      user_clean_init = DataCleaning(users_table=users_raw)
      cleaned_res = user_clean_init.clean_user_data()
      print(cleaned_res.head())
      db.upload_to_db(cleaned_dataframe=cleaned_res, table_name='dim_users', connection=local_engine)

 users_run()

 def cards_run():
      """
      The function "cards_run" retrieves data from a PDF file, cleans the data, and uploads it to a
      PostgreSQL database.
      """
   
      card_raw = de.retrieve_pdf_data(filepath=pdf_file)
      card_cleaned_init = DataCleaning(cards_table=card_raw)
      cleaned_cards = card_cleaned_init.clean_card_data()
      print("cards data cleaned")
      db.upload_to_db(cleaned_dataframe=cleaned_cards, table_name='dim_card_details', connection=local_engine)
      print("cards data uploaded to pgadmin4")
    
 cards_run()

 def stores_run():
      """
      The function `stores_run()` retrieves store data, cleans it, and uploads it to a database table.
      """
      headers = de.read_api_creds()
      stores_raw = de.retrieve_stores_data(endpoint=retrieve_store_endpoint, headers=headers)
      stores_clean_init = DataCleaning(stores_table=stores_raw)
      cleaned_stores = stores_clean_init.clean_store_data()
      db.upload_to_db(cleaned_dataframe=cleaned_stores, table_name='dim_store_details', connection=local_engine)

 stores_run()

 def products_run():
      """
      The function `products_run` extracts data from an S3 bucket, cleans the data, and uploads it to a
      database table.
      """
      
      products_raw = de.extract_from_s3(bucket=s3_bucket, file_from_s3=s3_object_key)
      product_clean_init = DataCleaning(products_table=products_raw)
      cleaned_products = product_clean_init.convert_product_weights()
      db.upload_to_db(cleaned_dataframe=cleaned_products, table_name='dim_products', connection=local_engine)
    
 products_run()

 def datetime_run():
      """
      The function `datetime_run` extracts datetime data from an S3 JSON file, cleans it, and uploads it
      to a database table.
      """
      
      datetime_raw = de.extract_from_s3_json(bucket=s3_json_bucket, file_from_s3=s3_json_object_key)
      datetime_clean_init = DataCleaning(datetimes_table=datetime_raw)
      cleaned_datetime = datetime_clean_init.clean_datetime_table()
      db.upload_to_db(cleaned_dataframe=cleaned_datetime, table_name='dim_date_times', connection=local_engine)
    
 datetime_run()
 print('All Extracting Done')
 print('All Cleaning Done') 
 print('All Uploading Done')

 """
  '''
     # 1. User details #
    # Assuming 'legacy_users' is a valid table in the database
    users_df = data_extractor.read_rds_table('legacy_users')

    # 2. Card details #
    # Using tabula-py package to return tabular data from PDF link as DataFrame
    pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df_card = data_extractor.retrieve_pdf_data(pdf_path)
    # .info() provides summary of data types and non-null values
    
    print(df_card.info())
    print(df_card.isna().mean() * 100)

    # 3. Store details #
    # Variables containing the two API endpoints required for GET requests
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    retrieve_stores_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    store_number = data_extractor.list_number_of_stores(number_of_stores_endpoint)
    print(store_number)
    number_of_stores = int(store_number['number_stores'])
    # Calling retrieve data method with API endpoint as argument to return store data in DataFrame in variable
    stores_df = data_extractor.retrieve_stores_data(retrieve_stores_data_endpoint, number_of_stores)
    
    print(stores_df.head())
    print(stores_df.info())

    # 4. Product details #
    s3_address = 's3://data-handling-public/products.csv'
    products_df = data_extractor.extract_from_s3(s3_address)
    if products_df is not None:
        print(products_df['weight'])  # Prints the weight column of the DataFrame

    # 5. Master orders table # (Using same extraction method used User details)
    orders_df = data_extractor.read_rds_table('orders_table')

    # 6. Date events details #
    dates_s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_events_df = data_extractor.extract_from_http_json(dates_s3_address)
    print(date_events_df.dtypes)
    print(date_events_df.head(2))
    '''

    ## Testing Extraction methods ##
    dates_s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_events_df = data_extractor.extract_from_http_json(dates_s3_address)

 """

 