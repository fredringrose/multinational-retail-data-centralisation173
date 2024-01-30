# Multinational Retail Data Centralisation Project

  # Database Connector

This code provides a class, `DatabaseConnector`, that facilitates connecting to a PostgreSQL database, reading data from it, and uploading data to it. It uses the `psycopg2` library to connect to the database and the `sqlalchemy` library to perform database operations.

## Getting Started

To use the `DatabaseConnector` class, you will need to create a YAML file that contains the credentials for your database. The YAML file should have the following format:

```yaml
RDS_USER: username
RDS_PASSWORD: password
RDS_HOST: hostname
RDS_PORT: port
RDS_DATABASE: database_name
```

Once you have created the YAML file, you can create an instance of the `DatabaseConnector` class and use it to connect to your database. The following code shows how to do this:

```python
import yaml
import pandas as pd
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

 # Data Extraction and Processing with Python

This project demonstrates various data extraction and processing techniques using Python. It includes methods to retrieve data from different sources such as relational databases, PDFs, APIs, S3, and HTTP JSON endpoints. The extracted data is then cleaned and processed to make it suitable for further analysis or storage.

## Prerequisites

To run this code, you will need the following:

- Python 3.11
- The following Python packages:
  - `tabula`
  - `requests`
  - `json`
  - `pandas`
  - `boto3`
  - `sqlalchemy`
  - `database_utils` (custom module containing the `DatabaseConnector` class)

## Step-by-Step Explanation

### 1. Database Connection

The code starts by importing the `DatabaseConnector` class from the `database_utils` module. This class provides methods to connect to a relational database using credentials stored in a YAML file.

```python
import os
from database_utils import DatabaseConnector

file_path = 'db_creds.yaml'
db_connector = DatabaseConnector(file_path)
```

### 2. Reading Data from a Database

The `read_rds_table` method of the `DataExtractor` class is used to read data from a table in the database. The method takes the table name as an argument and returns a pandas DataFrame containing the table data.

```python
def read_rds_table(self, table_name):
    # Access the engine object from the DatabaseConnector instance
    engine = self.database_connector.engine
    try:
        # Running SQL query and returning table in a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Error reading table '{table_name}': {e}")
        return None
```

### 3. Extracting Data from a PDF

The `retrieve_pdf_data` method of the `DataExtractor` class is used to extract tabular data from a PDF file. The method uses the `tabula` library to read the PDF and convert it into a pandas DataFrame.

```python
def retrieve_pdf_data(self, link): 
    # Use tabula to read the PDF file from

 # Data Cleaning and Loading Script

## Overview
This script performs data cleaning and loading operations on various datasets related to sales and user information. It connects to a PostgreSQL database, extracts data from various sources (including PDFs, JSON files, and APIs), cleans the data, and uploads it to the database in a structured and organized manner.

## Prerequisites
- Python 3.11 or later
- PostgreSQL database with the name 'sales_data' and a user with the necessary permissions
- The following Python libraries:
  - pandas
  - psycopg2
  - tabula-py
  - requests

## Step-by-Step Explanation

### 1. Importing Necessary Libraries
```python
import os
import requests
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype
import psycopg2
import re
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
```

### 2. Defining the `DataCleaning` Class
This class contains methods for cleaning different types of data, such as user data, card data, store data, product data, order data, and event data.

### 3. Cleaning User Data
The `clean_user_data` method performs the following operations on the user data:
- Removes duplicate columns
- Removes rows with null values
- Casts specific columns to the appropriate data types (string, datetime, etc.)

### 4. Cleaning Card Data
The `clean_card_data` method performs the following operations on the card data:
- Drops columns with more than 1000 null values
- Drops rows with any null values

### 5. Cleaning Store Data
The `called_clean_store_data` method performs the following operations on the store data:
- Drops columns with more than 100 null values
- Drops rows with any null values

### 6. Converting Product Weights
The `convert_product_weights` method converts the product weights from various units (e.g., grams, kilograms, milliliters) to a consistent unit (kilograms).

### 7. Cleaning Product Data
The `clean_products_data` method performs the following operations on the product data:
- Removes the first column (assuming it's a duplicate index)
- Removes rows with null or missing values

### 8. Cleaning Order Data

