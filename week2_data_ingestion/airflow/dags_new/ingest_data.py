import os
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, parquet_file, execution_date):
  print(table_name, parquet_file, execution_date)

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
  engine.connect()

  print('connection established successfully, inserting data...')

  parquet_dataset = pq.ParquetFile(parquet_file)

  for batch in parquet_dataset.iter_batches():
    batch_df = batch.to_pandas()
    break

  batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

  for batch in parquet_dataset.iter_batches():
    t_start = time()
    batch_df = batch.to_pandas()
    batch_df.to_sql(name=table_name, con=engine, if_exists="append")
    t_end = time()
    print("inserted another chunck, took %.3f second"%(t_end - t_start))