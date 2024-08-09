SELECT country_code AS country,
COUNT(country_code) AS total_num_stores
FROM dim_store_details
GROUP BY country 
ORDER BY total_num_stores DESC;

SELECT locality,
COUNT(locality) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 5;

SELECT ROUND(SUM(CAST(dim_products.product_price_sterling * orders_table.product_quantity AS numeric)), 2) AS total_sales,
month
FROM dim_date_times
INNER JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC
LIMIT 5;

ALTER TABLE dim_store_details
ADD COLUMN location VARCHAR(7);

UPDATE dim_store_details
SET location =
	CASE 
		WHEN store_type IN ('Local', 'Super Store', 'Mall Kiosk', 'Outlet') THEN 'Offline'
        WHEN store_type = 'Web Portal' THEN 'Web'
        ELSE NULL
	END; 

SELECT COUNT(product_quantity) AS number_of_sales,
SUM(product_quantity) AS product_quantity_count,
location
FROM dim_store_details
INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
GROUP BY location;

SELECT store_type,
ROUND(SUM(CAST(dim_products.product_price_sterling * orders_table.product_quantity AS numeric)), 2) AS total_sales,
ROUND(CAST(SUM(dim_products.product_price_sterling * orders_table.product_quantity) /
      SUM(SUM(dim_products.product_price_sterling * orders_table.product_quantity)) OVER () * 100 AS numeric), 2) AS percentage_total
FROM dim_store_details
INNER JOIN orders_table ON orders_table.store_code = dim_store_details.store_code
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY store_type
ORDER BY total_sales DESC;

SELECT 
ROUND(SUM(CAST(dim_products.product_price_sterling * orders_table.product_quantity AS numeric)), 2) AS total_sales, -- orders_table.store_code
year,
month
FROM dim_date_times
INNER JOIN orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY month, year
ORDER BY total_sales DESC 
LIMIT 5;

SELECT SUM(staff_numbers) AS total_staff_numbers,
country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

SELECT ROUND(SUM(CAST(dim_products.product_price_sterling * orders_table.product_quantity AS numeric)), 2) AS total_sales,
store_type,
dim_store_details.country_code
FROM dim_store_details
INNER JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
WHERE country_code LIKE 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY total_sales DESC
LIMIT 5;

WITH 
purchase_time_cte AS(
		SELECT year,
				month,
				day,
				timestamp,
		CAST(CONCAT(year, '-', month, '-', day, ' ', timestamp) AS TIMESTAMP) AS purchase_time
		FROM dim_date_times
		ORDER BY year, month, day, timestamp
	),	
next_purchase_time_cte AS(
	SELECT year,
			purchase_time,
			LEAD(purchase_time) OVER (PARTITION BY year ORDER BY purchase_time) AS next_purchase_time
	FROM purchase_time_cte
	ORDER BY year, purchase_time
	),
purchase_time_difference_cte AS (
	SELECT year,
			purchase_time,
			next_purchase_time,
	EXTRACT(EPOCH FROM(next_purchase_time - purchase_time)) AS purchase_time_difference
	FROM next_purchase_time_cte
	ORDER BY year, purchase_time, next_purchase_time
)
SELECT year,
CONCAT(
	'"hours": ', FLOOR(AVG(purchase_time_difference) / 3600), ', ',
	'"minutes": ', FLOOR((AVG(purchase_time_difference) % 3600) / 60), ', ',
	'"seconds": ', ROUND(AVG(purchase_time_difference) % 60), ', ',
	'"milliseconds": ', ROUND((AVG(purchase_time_difference)*1000)%1000)
) 
AS actual_time_taken
FROM purchase_time_difference_cte
GROUP BY year
ORDER BY AVG(purchase_time_difference) DESC
LIMIT 5;

