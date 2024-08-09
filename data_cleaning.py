from dateutil.parser import parse
import pandas as pd

class DatabaseCleaning:
    '''
    This class can be used to clean data from a variety of Amazon Web Services (AWS) data sources.

    '''

    def clean_user_data(self, user_df):
        '''
        This function is used to clean the user dataframe and return the cleaned dataframe.

        Args:
            user_df (pandas.DataFrame): the input dataframe containing user data. 

        Returns:
            pandas.DataFrame: the cleaned DataFrame.
        '''
        # Set index as index
        user_df = user_df.set_index('index')

        # Create mask and apply it to get rid of invalid rows and NULL/NaN
        selected_countries = ["Germany", "United Kingdom", "United States"]
        country_mask = user_df.loc[:,"country"].isin(selected_countries) # ,: means search all rows in just country
        user_df_mask = user_df[country_mask]

        ## convert most columns to string datatype
        # create dictionary to map column name to dataype
        col_data_types = {'first_name': 'string', 'last_name': 'string', 'company':'string', 'email_address':'string', 'address':'string', 'country':'string', 'country_code':'string','phone_number':'string','user_uuid':'string'}
        # use a for loop to iterate through the columns and change the dtype
        for column, data_type in col_data_types.items():
            user_df_mask[column] = user_df_mask[column].astype(data_type)

        ## convert date columns to datetime
        user_df_mask['date_of_birth'] = user_df_mask['date_of_birth'].apply(parse)
        user_df_mask['date_of_birth'] = user_df_mask['date_of_birth'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce')
        user_df_mask['join_date'] = user_df_mask['join_date'].apply(parse)
        user_df_mask['join_date'] = user_df_mask['join_date'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce') # .apply() applies a function, with arguments inside brackets
        # this prevents the rows being turned into nulls and then deleted.
    
        return user_df_mask

    def clean_card_data(self, card_df):
        '''
        This function is used to clean the card dataframe and return the cleaned dataframe.

        Args:
            card_df (pandas.DataFrame): the input dataframe containing card data. 

        Returns:
            pandas.DataFrame: the cleaned DataFrame.
        '''
        # mask to filter card_provider to delete invalid data and null rows
        card_provider_list = ["American Express","Diners Club / Carte Blanche", "Discover", "JCB 15 digit", "JCB 16 digit", "Maestro", "Mastercard", "VISA 13 digit", "VISA 16 digit", "VISA 19 digit"]
        card_mask = card_df.loc[:,"card_provider"].isin(card_provider_list)
        df_mask = card_df[card_mask]

        # drop the '?' characters in invalid card_numbers
        # to search specifically for character, convert to string
        df_mask['card_number'] = df_mask['card_number'].astype('string')
        replacements = [("?", "")]
        for char, replacement in replacements:
            df_mask["card_number"] = df_mask["card_number"].str.replace(char, replacement)
        # convert into int64
        df_mask['card_number'] = df_mask['card_number'].astype('int64')

        # turn other cols into strings 
        col_data_types = {'expiry_date':'string', 'card_provider':'string'}
        for column, data_type in col_data_types.items():
            df_mask[column] = df_mask[column].astype(data_type)
        
        # cast columns to datetime
        df_mask['date_payment_confirmed'] = df_mask['date_payment_confirmed'].apply(parse)
        df_mask['date_payment_confirmed'] = df_mask['date_payment_confirmed'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce') 

        return df_mask

    def clean_store_data(self, store_df):
        '''
        This function is used to clean the store dataframe and return the cleaned dataframe.

        Args:
            store_df (pandas.DataFrame): the input dataframe containing store data. 

        Returns:
            pandas.DataFrame: the cleaned DataFrame.
        '''
        # drop empty lat column
        store_info_df_no_lat = store_df.drop('lat', axis=1)

        # create store_type mask for cleaning strange data and nulls 
        store_type_list = ["Local", "Mall Kiosk", "Super Store", "Outlet", "Web Portal"]
        store_type_mask = store_info_df_no_lat.loc[:,"store_type"].isin(store_type_list)
        store_info_mask = store_info_df_no_lat[store_type_mask]
    
        # ValueError: could not convert string to float: 'N/A'
        # drop N/A from longitude or latitude at index 0
        store_info_mask_nona = store_info_mask.drop([0], axis=0) # had to re-enter this row in SQL

        # convert columns to string and flaot64
        col_data_types = {"latitude":"float64", "longitude":"float64", "address":"string", "locality":"string", "store_code":"string", "store_type":"string", "country_code":"string", "continent":"string"}
        for column, data_type in col_data_types.items():
            store_info_mask_nona[column] = store_info_mask_nona[column].astype(data_type)

        # staff_number has values with "accidental" letters mixed in so can't convert to int64
        store_info_mask_nona['staff_numbers'] = store_info_mask_nona['staff_numbers'].replace(["J78", "30e", "80R", "A97", "3n9"], ["78", "30", "80", "97", "39"])
        # convert to int64
        store_info_mask_nona['staff_numbers'] = store_info_mask_nona['staff_numbers'].astype("int64")
        #print(store_info_mask_nona.info())

        # change opening_date to datetime64
        store_info_mask_nona['opening_date'] = store_info_mask_nona['opening_date'].apply(parse)
        store_info_mask_nona['opening_date'] = store_info_mask_nona['opening_date'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce') 

        return store_info_mask_nona


    def convert_product_weights(self, product_df):
        '''
        This function is used to convert the weights column from mixed units to solely kg units within the products dataframe and return the dataframe with converted column.

        Args:
            product_df (pandas.DataFrame): the input dataframe containing product data. 

        Returns:
            pandas.DataFrame: the DataFrame with weight column converted to kg.
        '''
        # create new columns, using regex to seperate weight and unit
        product_df['numeric_value'] = pd.to_numeric(product_df['weight'].str.extract('(\d+.\d+|\d+)')[0], errors='coerce') # casts as a float 
        product_df['unit'] = product_df['weight'].str.extract('([a-zA-Z]+)')
        
        # define dictionary for conversion
        conversion_factors = {'ml': 0.001, 'g': 0.001, 'kg': 1, 'k': 1}
        
        product_df.loc[product_df['unit'] == 'ml', 'numeric_value'] *= conversion_factors['ml']
        # for every row where ['unit'] equals ml, it multiplies the corresponding 'numeric_value' row by the value of the 'ml' key in the conversion_factors dictionary
        product_df.loc[product_df['unit'] == 'g', 'numeric_value'] *= conversion_factors['g']
        product_df.loc[product_df['unit'].isin(['kg', 'k']), 'numeric_value'] *= conversion_factors['kg']

        product_df['weight_kg'] = product_df['numeric_value']

        # drop unnecessary columns
        product_df.drop(['weight', 'numeric_value', 'unit'], axis=1, inplace=True)
        
        return product_df
    
    def clean_products_data(self, product_df):
        '''
        This function is used to clean the product dataframe and return the cleaned dataframe.

        Args:
            product_df (pandas.DataFrame): the input dataframe containing product data. 

        Returns:
            pandas.DataFrame: the cleaned DataFrame.
        '''
        # create mask to filter invalid data using removed column:
        # first correct spelling mistake of 'avaliable'
        product_df["removed"] = product_df["removed"].astype("string")
        product_df["removed"] = product_df["removed"].str.replace('Still_avaliable', 'Still_available', regex=True)
        # filter
        availability_list = ["Still_available", "Removed"] 
        availability_mask = product_df.loc[:,"removed"].isin(availability_list)
        product_mask_df = product_df[availability_mask]

        # remove £ from price column and convert to float64
        # cast as string
        product_mask_df["product_price"] = product_mask_df["product_price"].astype("string")
        # create new col from it while removing the £ sign
        product_mask_df["product_price_sterling"] = product_mask_df["product_price"].str.replace("£", "", regex=True)
        # convert to float
        product_mask_df["product_price_sterling"] = product_mask_df["product_price_sterling"].astype(float)
        # delete 'product_price' column
        product_mask_df = product_mask_df.drop(["product_price"], axis=1)

        # cast columns to correct datatype apart from datetime
        col_data_types = {'product_name':'string', 'category':'string', 'EAN':'string', 'uuid':'string', 'product_code':'string'}
        for column, data_type in col_data_types.items():
            product_mask_df[column] = product_mask_df[column].astype(data_type)

        # make all product_code upper()
        product_mask_df["product_code"] = product_mask_df["product_code"].str.upper()
            
        # impute data for the weights which are 0kg, using mean
        #product_mask_df["weight_kg"].describe()
        product_mask_df["weight_kg"] = product_mask_df["weight_kg"].fillna(product_mask_df["weight_kg"].mean())  # mean = 3.15

        # cast datetime
        product_mask_df['date_added'] = product_mask_df['date_added'].apply(parse)
        product_mask_df['date_added'] = product_mask_df['date_added'].apply(pd.to_datetime, infer_datetime_format=True, errors='coerce') 
        
        return product_mask_df

    def clean_orders_data(self, orders_df):
        '''
        This function is used to clean the orders dataframe and return the cleaned dataframe.

        Args:
            orders_df (pandas.DataFrame): the input dataframe containing orders data. 

        Returns:
            pandas.DataFrame: the cleaned DataFrame.
        '''
        # drop unnecessary columns 
        df_dropped_cols = orders_df.drop(["first_name", "last_name", "1"], axis=1) 
        
        # make product_code upper case
        df_dropped_cols["product_code"] = df_dropped_cols["product_code"].str.upper()
        
        # cast other columns to string
        col_data_types = {"date_uuid":"string", "user_uuid":"string", "store_code":"string", "product_code":"string"}
        for column, data_type in col_data_types.items():
            df_dropped_cols[column] = df_dropped_cols[column].astype(data_type)
        
        return df_dropped_cols
    

    def clean_date_data(self, date_df):
        '''
        This function is used to clean the date dataframe and return the cleaned dataframe.

        Args:
            date_df (pandas.DataFrame): the input dataframe containing date data. 

        Returns:
            pandas.DataFrame: the cleaned DataFrame.
        '''
        # use mask to remove invalid data 
        # use time_period as has fewest variables 
        time_period_list = ["Evening", "Morning", "Late_Hours", "Midday"]
        time_period_mask = date_df.loc[:,"time_period"].isin(time_period_list)
        time_df_mask = date_df[time_period_mask]

        # create new column as an amalgamation of month year day
        time_df_mask['purchase_date'] = pd.to_datetime(time_df_mask["year"] + "-" + time_df_mask["month"]+ "-" + time_df_mask["day"])

        # cast timestamp and purchase_date as strings to combine
        time_df_mask["timestamp"] = time_df_mask["timestamp"].astype("string")
        time_df_mask["purchase_date"] = time_df_mask["purchase_date"].astype("string")

        # combine and convert to datetime as a new column
        time_df_mask["purchase_datetime"] = pd.to_datetime(time_df_mask["purchase_date"] + " " + time_df_mask["timestamp"])

        # recast the date column as datetime
        time_df_mask["purchase_date"] = pd.to_datetime(time_df_mask["purchase_date"])

        # cast the other columns as strings
        col_data_types = {"month": "int32", "year":"int32", "day":"int32", "time_period": "string", "date_uuid": "string"}
        for column, data_type in col_data_types.items():
            time_df_mask[column] = time_df_mask[column].astype(data_type)
        
        return time_df_mask 
