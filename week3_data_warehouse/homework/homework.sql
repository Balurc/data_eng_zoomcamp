-- What is count for fhv vehicles data for year 2019?

-- Load data from cloud storage to an external table
CREATE OR REPLACE EXTERNAL TABLE `your_project_id.trips_data_all.external_fhv_tripdata`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://your_bucket_name/raw/fhv_tripdata/2019/fhv_tripdata_2019-*.parquet']
    );

-- Query: count for fhv vehicles data for year 2019
SELECT count(*) 
FROM `trips_data_all.external_fhv_tripdata`;

-- How many distinct dispatching_base_num we have in fhv for 2019?
SELECT COUNT(DISTINCT(dispatching_base_num)) 
FROM `trips_data_all.external_fhv_tripdata`;

-- Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num?
-- Partition by dropoff_datetime & cluster by dispatching_base_num
CREATE OR REPLACE TABLE `your_project_id.trips_data_all.fhv_tripdata_clustered1`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `trips_data_all.external_fhv_tripdata`
);

-- What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279

-- Create a clustered table by dispatching_base_num and partitioned by pickup_datetime
CREATE OR REPLACE TABLE `your_project_id.trips_data_all.fhv_tripdata_clustered2`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `trips_data_all.external_fhv_tripdata`
);

-- Query: select all records based on the conditions
SELECT COUNT(*)
FROM `trips_data_all.fhv_tripdata_clustered2`
WHERE DATE(pickup_datetime) BETWEEN "2019-01-01" AND "2019-03-31"
  AND dispatching_base_num IN ('B00987','B02060','B02279');

-- What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag?
SELECT COUNT(DISTINCT(SR_Flag))
FROM `trips_data_all.fhv_tripdata`
WHERE DATE(pickup_datetime) BETWEEN "2019-01-01" AND "2019-03-31";