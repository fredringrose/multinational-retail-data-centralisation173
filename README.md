# Multinational Retail Data Centralisation Project
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) 
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)

# Table of Contents
1. [Introduction](#introduction)
2. [Installation](#installation)
3. [File Structure](#file-structure)
4. [Personal Reflection](#personal-reflection)
   1. [Data Security](#data-security)
   2. [Data Extraction](#data-extraction)
   3. [Data Cleaning](#data-cleaning)
   4. [SQL](#sql)
   5. [Matplotlib](#matplotlib)
   6. [Conclusion](#conclusion)
7. [Licence Information](#licence-information)

## Introduction 

Welcome to the Multinational Retail Data Centralisation Project! 

This project is part of my journey through the Cloud Engineering pathway at AiCore. The projects aim is to centralise and query data from a multinational retail business using cloud technologies.

The script retrieves data from a multitude of sources on AWS, including an Amazon Relational Database Service (RDS) instance, a PDF document, JSON and CSV files in an AWS S3 bucket, and through interactions with an API. Following data retrieval, the script cleans these DataFrames and uploads them to a PostgreSQL database. SQL scripts are then run to generate the schema and query the business model.

Here is the generated Entity-Relationship Diagram (ERD) for the sales_data database.

![ERD of sales_data](https://i.imgur.com/mKZaZOp.png)

## Installation 

Create a folder on your local machine where you want this repo to be in and move into it.
Clone the repository into this folder on your local machine by running the following command inside your terminal:

```
git clone https://github.com/Claudiomics/multinational-retail-data-centralisation-project.git
```
Then move into the folder and run the following code:
```
python3 main.py
```

To upload to the sales_data database and query the database through SQL scripts, the database needed to be initialised and connected to:

- Right click on Databases in PgAdmin4 and create sales_data
- In VSCode install the SQL Tools extension
- To connect to PostgreSQL, open the extension and configure connection by entering the connection details, including host, port, username, password, and database name

To retrieve the data from the S3 bucket, the CLI needed to be connected to:

- Install AWS CLI
- Configure AWS CLI by typing the following into the terminal
```
aws configure
```
- Enter your AWS Access Key ID, Secret Access Key, default region, and default output format to access the S3 bucket

## Libraries and Modules Required for This Project

- [PyYAML](#https://pypi.org/project/PyYAML/) - Used for reading YAML files that contain credentials and configuration settings
```
pip install pyyaml
```
- [Psycopg2](#https://pypi.org/project/psycopg2/) - Used for connecting to the PostgreSQL database and executing SQL queries
```
pip install psycopg2-binary
```
- [SQLAlchemy](#https://www.sqlalchemy.org/) - Used for creating a SQLAlchemy database engine to connect to the database
```
pip install sqlalchemy
```
- [Pandas](#https://pandas.pydata.org/) - Used to create DataFrames, clean them and transform them
```
pip install pandas 
```
- [Dateutil](#https://pypi.org/project/python-dateutil/) - Used to parse date strings during data cleaning
```
pip install python-dateutil
```
- [Boto3](#https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - Used for interacting with AWS S3 to extract data from an S3 bucket
```
pip install boto3
```
- [Requests](#https://pypi.org/project/requests/) - Used to retrieve data from an API endpoint
```
pip install requests
```
- [Tabula](#https://pypi.org/project/tabula-py/) - Used to extract data from a PDF file
```
pip install tabula-py
```
- [NumPy and MatPlotLib](#https://matplotlib.org/) - Used to generate a pie chart visualization of the percentage of sales by store type
```
pip install numpy matplotlib
```

## File Structure 
```
.
├── README.md
├── __pycache__
│   ├── data_cleaning.cpython-311.pyc
│   ├── data_extraction.cpython-311.pyc
│   └── database_utils.cpython-311.pyc
├── api_key.yaml
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── db_creds.yaml
├── json_s3_url.yaml
├── main.py
├── my_creds.yaml
├── s3_url.yaml
└── sql_files
    ├── essential_queries
    │   ├── business_queries.sql
    │   └── create_schema.sql
    └── with_notes
        ├── db_query_notes.sql
        └── db_schema_notes.sql
```

## Personal Reflection

### Data Security

In order to upload changes to github without hard coding database credentials and API keys into the script, I used separate YAML files which contain sensitive information to prevent being a victim of web scraping. These files were put in a .gitignore file so they were not uploaded to my remote github repository. 

In future to increase security, I would use [GitGuardian](#https://docs.gitguardian.com/) to alert me if I try to upload credentials to github and encrypt my environmental variables using the .gitignore file and the python decoupling module as shown below: 

![Python Security Workflow](https://i.imgur.com/EOWXpeV.png)
(Python Security Workflow created by Claudiomics on Canva 2024-01-10)

### Data Extraction

Developing methods to extract data from many different sources was a challenge to me, but one I learned a great deal from. 

I developed the **read_db_creds** and **init_db_engine** methods to be able to utilise the sensitive information stored in YAML files and be able to create an engine. As read_db_creds returns a dictionary, the init_db_engine uses this method to extract the relevant information and assemble the string in the correct order before creating an engine and connecting to SQAlchemy. This was used in conjunction with a YAML file containing the credentials to connect to an AWS RDS database.

To retrieve the data stored in the RDS, firstly I developed the **list_db_tables** method to list the names of the 'public' tables within so you can specify which table to extract data from when retrieving it. This method originally returned a tuple, so required a list comprehension to return only the first element of each tuple in the **list_of_tables** variable. The result of this **list_db_tables** method was used to get the user data table name and retrieve the data using the **read_rds_table** method which uses pandas to convert and return the table into a dataframe.

This data was cleaned before extracting the card data which was stored in a PDF document in an AWS S3 bucket. This used the **retrieve_pdf_data** method which used tabula to read a remote PDF into a pandas dataframe. I struggled to understand that tabula returns a list of DataFrames (where each DataFrame represents one page from the PDF) but once this was clear to me (thanks Jared) I added a line of code to concatenate this list into a singular DataFrame. 

Next, I used an API and it's two GET methods to first retrieve the number of stores in the database (**list_number_of_stores**) which returned the number of stores in the database. I then used the returned integer and a _for loop_ to iterate through and construct the full endpoint for whichever store it was retrieving information from in the **retrieve_stores_data** method. With this constructed endpoint, the method uses the second GET request to extract the data from each store, before converting the data into python data structure, appending it to a list and then converting into a Pandas DataFrame. 

To extract product data and date details data from a CSV file and a JSON file, respectively, which were stored in an S3 bucket I developed the **extract_from_s3** method. This method creates an S3 client object using boto3 for AWS interactions and splices the S3 address into the bucket name and object key so they could be used to retrieve the content of the specified object before converting the data into a Pandas DataFrame. This method includes _if elif_ to specify the file extension.

Finally, I reused the **read_rds_table** method to extract the orders data from the RDS database.

Personally, I found developing these extraction methods the most difficult part of this project but am happy with how they have turned out and the knowledge I have gained. 

### Data Cleaning

At first I struggled with developing the cleaning methods as I was unable to fully visualise the uncleaned DataFrame to see what parts of the code needed changing. I therefore installed the 'Excel Viewer' into VSCode to be able to view the data in full to be able to develop the different cleaning methods before developing the actual method in the DatabaseCleaning class. I have detailed this below:

- Within VSCode, download the extension called 'Excel Viewer'
- After you have retrieved your unclean DataFrame (unclean_df), transfer it into a CSV file
```
unclean_df.to_csv('unclean_df.csv')
```
- Run this script to generate a csv file of the unclean_df
- Right click on this file and open with Excel Viewer and explore the data
- Create an .ipynb file and name it something like trial_cleaning
- Load in the unclean_df to the trial_cleaning.ipynb
```
import pandas as pd

unclean_df = pd.read_csv('unclean_df.csv')
print(unclean_df.head(20))
```
- Use this to explore the data types and experiment on the DataFrame without worrying about deleting important information.
- When happy with the .ipynb code, develop the cleaning method in the DatabaseCleaning class, returning the cleaned DataFrame for use in the main.py file
- Call the method to clean the dataframe
- Create a new CSV file of the cleaned DataFrame and open it in Excel Viewer to check how clean it looks
```
clean_df.to_csv('clean_df.csv')
```
When exploring the data, I noticed across all tables that some rows included invalid data which needed removing, along with any columns with NULL. I used a mask in all my cleaning methods to filter the data and return only the rows which contained valid data. This took a while for me to figure out, as before, I had tried delete these rows individually by searching for the specific data that was causing the error and deleting the rows one-by-one. The use of the mask was very efficient and removed NULL rows without me having to run that as a separate command.

I used a _for loop_ and a column-type dictionary to convert the datatypes of the rows to the correct types in one go. In my **clean_user_data** method, this for loop didn't work for the date columns, so I converted these separately as some of the data was in a format that was unreadable. I used the parse function from dateutils to deal with this.

Within the **clean_card_data** there were many card numbers that contained question marks, which I removed using a _for loop_ and a replace statement. Similarly, in **clean_store_data** there were staff numbers with letters added in, which I replaced manually before being able to cast the column to an integer.

The **convert_product_weights** was used to convert a column from the products DataFrame with mixed-unit weights into just weights as kg. This required the use of regex, which I had limited exposure to previously. AiCore's Ismael was able to direct me to [RegexOne](#https://regexone.com/) which I used in conjunction with [regex101](#https://regex101.com/) to develop code which separates the numeric weight and unit for conversion to kg.

**clean_products_data** was a method that corrected a spelling mistake in an entire column, and imputed weight data where the weight was 0kg with the mean of the entire column.

The **clean_orders_data** method only dropped three empty columns alongside the standard mask and data type conversions.

Lastly, my **clean_date_data** method created a new column with the entire datetime, as these were separated into different columns. However, the columns I created turned out not to be necessary during the SQL query section, so in future I wouldn't add these columns until I knew I needed them.

After all of the cleaning methods were set up, I developed the **upload_to_db** method to upload the cleaned DataFrames to PostgresQL. This uses the **init_db_engine** to initialise the engine using my_creds.yaml containing the information to connect to the sales_data database and then is used to upload the DataFrame to a specified table.

Overall, I would have liked to troubleshoot my methods a bit more as I get a lot of 'A value is trying to be set on a copy of a slice from a DataFrame' messages when running the script. However, the code works for now and I will look into improving on this in future projects.

### SQL 

I was able to set up and manage the sales_data PostgreSQL database including schema creation and SQL queries using SQLAlchemy through VSCode. I had some trouble trying to create the connection without typing my password and other credentials directly into the connection string, however I managed to do this by using the **init_db_creds** method to retrieve the values of my key:value pairs returned from the **init_db_creds** dictionary. When I had figured out how to run an SQL file from my main python file, I took time to develop the function to return the data in a way that would be easy for the user to read and understand. I also included try, except blocks to handle any queries that wouldn't be able to run. I am very pleased with how this runs, however in future, I would try to merge the **execute_schema_sql_file** and **execute_query_sql_file** functions together as I repeat some code.

The last query to retrieve business insights gave me some trouble as it took me a while to realise I needed to use a common table expression (cte) to return the desired result. I also learned to understand the LEAD() function which uses the previous entry in the column before to calculate new information. Additionally, as my **execute_query_sql_file** had previously only run queries that started with 'SELECT' I had to adapt it to also execute 'WITH'.

### Matplotlib 

I included this part of code into my project as a bit of extra fun. It was my first exposure to matplotlib as I think it's very important to be able to visualise data when presenting to a business owner. I used the output of query five to create a pie chart that pops up when the code is run which shows the percentage of sales each store type returns. Unfortunately, I couldn't figure out how to automatically generate this from the output of the SQL query, so I needed to input the data manually, however in future this is something that I would aim to do. Lastly, I don't know why the chart title doesn't show up on the graph, which I would like to include in future.

### Conclusion 

Overall, I am happy with how I completed this project. It was a steep learning curve and many times I felt overwhelmed with all the things I had to learn. I was able to retrieve data from cloud resources such as an AWS RDS, a CSV, JSON and PDF from an S3 bucket, and used an API. I managed to visualise the data effectively and develop cleaning method for each DataFrame. I learned the basics of RegEx and how to keep any sensitive information from being uploaded to github, while still using the values in the script. My main.py script looks aesthetically pleasing and easy to follow as I have defined the functions all in one go at the beginning of the document and used an **if __name__ == "__main__":** block to run the actual script in one go.

In future, I would improve my data cleaning skills so I don't get any warning messages and possibly extract more of the code in my main.py file so it's easier to understand. I'd also like to be able to generate an Entity-Relationship Diagram (ERD) of the database using python, as this is something I tried to do but failed. 

Thanks to all the AiCore staff for guiding me through this project when I needed help, I really appreciate it.

## Licence Information

 Copyright (C) 2007 Free Software Foundation, Inc. <https://fsf.org/>
 Everyone is permitted to copy and distribute verbatim copies
 of this license document, but changing it is not allowed.



