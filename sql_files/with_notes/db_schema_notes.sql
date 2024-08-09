/* 
This file will be used to run SQL queries on the sales_data database to create the database schema.
1 - 7: Changing tables
8: Assigning primary keys
9: Assigning foreign keys*/

/* 1. Cast the column in orders_table to the correct datatypes:
+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+*/

--orders_table
SELECT * FROM orders_table;

-- drop index columns
ALTER TABLE orders_table
DROP COLUMN level_0,
DROP COLUMN index;

-- check max length of card_number for varchar
SELECT MAX(LENGTH(card_number)) AS max_length
FROM orders_table;

-- check dtypes as length(bigint) does not exist
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'orders_table'; -- it is in fact a bigint...

-- order card_number column DESC order and count longest row
SELECT *
FROM orders_table
ORDER BY card_number DESC; -- 19 characters long

-- check max length of store_code for varchar
SELECT MAX(LENGTH(store_code)) AS max_length
FROM orders_table; -- 12

-- check max length of product_code for varchar
SELECT MAX(LENGTH(product_code)) AS max_length
FROM orders_table; -- 11

-- change datatypes 
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT; -- recheck dtypes

/* 2. Cast the column in dim_users to the correct datatypes:
+----------------+--------------------+--------------------+
| dim_user_table | current data type  | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR(255)       |
| last_name      | TEXT               | VARCHAR(255)       |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(?)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |
+----------------+--------------------+--------------------+*/

--dim_users
SELECT * FROM dim_users;

-- check max length of country_code for varchar
SELECT MAX(LENGTH(country_code)) AS max_length
FROM dim_users; -- 3

-- change datatypes 
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE;

-- check dtypes
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

/* 3. Cast the column in dim_store_details to the correct datatypes:
+---------------------+-------------------+------------------------+
| store_details_table | current data type |   required data type   |
+---------------------+-------------------+------------------------+
| longitude           | TEXT              | FLOAT                  |
| locality            | TEXT              | VARCHAR(255)           |
| store_code          | TEXT              | VARCHAR(?)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
| latitude            | TEXT              | FLOAT                  |
| country_code        | TEXT              | VARCHAR(?)             |
| continent           | TEXT              | VARCHAR(255)           |
+---------------------+-------------------+------------------------+*/

--dim_store_details
SELECT * FROM dim_store_details;

-- drop index col
ALTER TABLE dim_store_details
DROP COLUMN index;

-- check max length of store_code for varchar
SELECT MAX(LENGTH(store_code)) AS max_length
FROM dim_store_details; -- 11

-- check max length of country_code for varchar
SELECT MAX(LENGTH(country_code)) AS max_length
FROM dim_store_details; -- 2

-- change datatypes 
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(11),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

-- check dtypes
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';

/* 4. Update dim_products table for the delivery team.
The product_price column has a £ character which you need to remove using SQL.
Add a new column weight_class which will contain human-readable values based on the weight range of the product.
+--------------------------+-------------------+
| weight_class VARCHAR(?)  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+*/

-- Already cleaned out the £ character while cleaning but here's what I'd do:
/*
UPDATE your_table
SET price_column = REPLACE(price_column, '£', '')
WHERE price_column LIKE '£%'; */

-- dim_products
SELECT * FROM dim_products;

-- New column with descriptive weight classes
-- Create new column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);
-- Fill it with a case
UPDATE dim_products
SET weight_class = 
  CASE 
    WHEN weight_kg >= 0 AND weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    WHEN weight_kg >= 140 THEN 'Truck_Required'
  END;

-- Check
SELECT weight_kg, weight_class 
FROM dim_products
ORDER BY weight_kg DESC; 

/* 5. Cast the columns in dim_products to the correct datatypes:
+-----------------+--------------------+--------------------+
|  dim_products   | current data type  | required data type |
+-----------------+--------------------+--------------------+
| product_price   | TEXT               | FLOAT              |
| weight          | TEXT               | FLOAT              |
| EAN             | TEXT               | VARCHAR(?)         |
| product_code    | TEXT               | VARCHAR(?)         |
| date_added      | TEXT               | DATE               |
| uuid            | TEXT               | UUID               |
| still_available | TEXT               | BOOL               |
| weight_class    | TEXT               | VARCHAR(?)         |
+-----------------+--------------------+--------------------+*/

-- dim_products
SELECT * FROM dim_products;

-- drop index column
ALTER TABLE dim_products
DROP COLUMN "Unnamed: 0";

-- check max length of EAN for varchar
SELECT MAX(LENGTH("EAN")) AS max_length
FROM dim_products; -- 17

-- check max length of product_code for varchar
SELECT MAX(LENGTH(product_code)) AS max_length
FROM dim_products; -- 11

-- check max length of weight_class for varchar
SELECT MAX(LENGTH(weight_class)) AS max_length
FROM dim_products; -- 14

-- Change column name removed to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- change datatypes apart from bool
ALTER TABLE dim_products
ALTER COLUMN product_price_sterling TYPE FLOAT,
ALTER COLUMN weight_kg TYPE FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN weight_class TYPE VARCHAR(14); 

-- change still_available column to bool
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING 
  CASE WHEN still_available = 'Still_available' THEN TRUE ELSE FALSE END;

/* 6. Cast the columns in dim_date_times to the correct datatypes:
+-----------------+-------------------+--------------------+
| dim_date_times  | current data type | required data type |
+-----------------+-------------------+--------------------+
| month           | TEXT              | VARCHAR(?)         |
| year            | TEXT              | VARCHAR(?)         |
| day             | TEXT              | VARCHAR(?)         |
| time_period     | TEXT              | VARCHAR(?)         |
| date_uuid       | TEXT              | UUID               |
+-----------------+-------------------+--------------------+*/

-- dim_date_times
SELECT * FROM dim_date_times;

-- check max length of country_code for varchar
SELECT MAX(LENGTH(time_period)) AS max_length
FROM dim_date_times; -- 10

-- change datatypes apart from bool
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid; 

/* 6. Cast the columns in dim_card_details to the correct datatypes:
+------------------------+-------------------+--------------------+
|    dim_card_details    | current data type | required data type |
+------------------------+-------------------+--------------------+
| card_number            | TEXT              | VARCHAR(?)         |
| expiry_date            | TEXT              | VARCHAR(?)         |
| date_payment_confirmed | TEXT              | DATE               |
+------------------------+-------------------+--------------------+*/
-- dim_card_details
SELECT * FROM dim_card_details;

-- check max length of card_number for varchar
SELECT MAX(LENGTH(card_number)) AS max_length
FROM dim_card_details; --  bigint

-- order card_number column DESC order and count longest row
SELECT *
FROM dim_card_details
ORDER BY card_number DESC; -- 19 characters long (same as orders table)

-- check max length of expiry_date for varchar
SELECT MAX(LENGTH(expiry_date)) AS max_length
FROM dim_card_details; -- 5

-- change datatypes 
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

/* 7. Update the respective columns as primary key columns */

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

/* 8. Create the foreign keys in the orders_table to reference the primary keys in the dim tables.*/

SELECT * FROM orders_table;

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_users
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code); --error dure to deleting one store during cleaning

-- re-add row back in
-- change dtype
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);
-- insert row
INSERT INTO dim_store_details (address, longitude, locality, store_code, staff_numbers, opening_date, store_type, latitude, country_code, continent)
VALUES (NULL, NULL, NULL, 'WEB-1388012W', 325,'2010-06-12', 'Web Portal', NULL, 'GB', 'Europe');
-- check 
SELECT * FROM dim_store_details
WHERE store_code = 'WEB-1388012W'; -- yes, go back and re-run foreign key assignment

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);


