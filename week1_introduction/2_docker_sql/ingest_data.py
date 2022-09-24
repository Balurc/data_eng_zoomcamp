import os
import argparse
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_taxi = params.table_name_taxi
    table_name_zones = params.table_name_zones
    url_taxi = params.url_taxi
    url_zones = params.url_zones
    yellow_pq_name = "output.parquet"
    zones_csv_name = "output.csv"

    # Download the data
    print("Downloading yellow taxi trips data")
    os.system(f"wget {url_taxi} -O {yellow_pq_name}")
    print("Downloading taxi zones data")
    os.system(f"wget {url_zones} -O {zones_csv_name}")

    # Ingesting yellow taxi trips data - Januari 2021
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    parquet_file = pq.ParquetFile(yellow_pq_name)

    print("Starting yellow taxi trips data ingestion to pg database")
    for batch in parquet_file.iter_batches():
        batch_df = batch.to_pandas()
        break

    batch_df.head(n=0).to_sql(name=table_name_taxi, con=engine, if_exists="replace")

    for batch in parquet_file.iter_batches():
        t_start = time()
        
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table_name_taxi, con=engine, if_exists="append")
        t_end = time()
        
        print("inserted another chunck, took %.3f second"%(t_end - t_start))
    print("Finished yellow taxi trips data ingestion to pg database")
    
    # Ingesting zones information
    print("Starting taxi zones data ingestion to pg database")
    zones_df = pd.read_csv(zones_csv_name)
    zones_df.to_sql(name=table_name_zones, con=engine, if_exists='replace')
    print("Finished taxi zones data ingestion to pg database")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest data to Postgres")

    # user, password, host, port, database name, table name, url of the data
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name_taxi", help="table name taxi for postgres")
    parser.add_argument("--table_name_zones", help="table name zones for postgres")
    parser.add_argument("--url_taxi", help="url taxi of the data")
    parser.add_argument("--url_zones", help="url taxi zones of the data")

    args = parser.parse_args()

    main(args)