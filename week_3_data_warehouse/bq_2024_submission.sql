-- Creating the external table
CREATE OR REPLACE TABLE robotic-vista-412114.ny_taxi.external_green_2022_non_partitioned AS
SELECT VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
FROM `robotic-vista-412114.ny_taxi.external_green_2022`;

--Q1
SELECT COUNT(VendorID)
FROM `robotic-vista-412114.ny_taxi.external_green_2022`;

--Q2
SELECT DISTINCT COUNT(PULocationID)
FROM `robotic-vista-412114.ny_taxi.external_green_2022`;

SELECT DISTINCT COUNT(PULocationID)
FROM `robotic-vista-412114.ny_taxi.external_green_2022_non_partitioned`;

--Q3
SELECT COUNT(fare_amount)
FROM `robotic-vista-412114.ny_taxi.external_green_2022`
WHERE fare_amount = 0;

--Q4
CREATE OR REPLACE TABLE robotic-vista-412114.ny_taxi.external_green_2022_partitioned_clustered
PARTITION BY
DATE(lpep_pickup_datetime)
CLUSTER BY
  PULocationID AS
SELECT *
FROM `robotic-vista-412114.ny_taxi.external_green_2022_non_partitioned`;

--Q5
SELECT DISTINCT(PULocationID)
FROM `robotic-vista-412114.ny_taxi.external_green_2022_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';

SELECT DISTINCT(PULocationID)
FROM `robotic-vista-412114.ny_taxi.external_green_2022_partitioned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30';