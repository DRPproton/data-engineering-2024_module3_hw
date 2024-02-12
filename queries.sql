-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `my-mage-project.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-bucket-dash/green_taxi_data_2022/green_tripdata_2022-*.parquet']
);


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE my-mage-project.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM my-mage-project.ny_taxi.external_green_tripdata;

-- Question 1 
-- Maount of rows
SELECT COUNT(*) FROM my-mage-project.ny_taxi.external_green_tripdata;


-- Question 2
SELECT COUNT(DISTINCT(PULocationID)) FROM my-mage-project.ny_taxi.external_green_tripdata;

SELECT COUNT(DISTINCT(PULocationID)) FROM my-mage-project.ny_taxi.green_tripdata_non_partitoned;


-- Question 3
SELECT COUNT(fare_amount) FROM my-mage-project.ny_taxi.green_tripdata_non_partitoned
WHERE fare_amount = 0;

-- Question 4
CREATE OR REPLACE TABLE my-mage-project.ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE (lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM my-mage-project.ny_taxi.external_green_tripdata;

-- Question 5
SELECT DISTINCT(PULocationID) FROM my-mage-project.ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID) FROM my-mage-project.ny_taxi.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Questin 8
SELECT COUNT(*) FROM my-mage-project.ny_taxi.green_tripdata_non_partitoned;