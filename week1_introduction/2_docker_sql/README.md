services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volume:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restrart: always

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /Users/baluramachandra/Desktop/data_eng_zoomcamp/week1_introduction/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5431:5432
  postgres:13

  pgcli -h localhost -p 5431 -u root -d ny_taxi

  https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
  https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
  https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv



  docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4


# Network

docker network create pg-network

  docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /Users/baluramachandra/Desktop/data_eng_zoomcamp/week1_introduction/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5431:5432
  --network=pg-network \
  --name pgdatabase \
  postgres:13



    docker run -it \

  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \

  -e PGADMIN_DEFAULT_PASSWORD="root" \

  -p 8080:80 \
  --network=pg-network \
  --name pgadmin2 \

  dpage/pgadmin4

  jupyter nbconvert --to=script


  python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5431 \ 
    --db=ny_taxi \ 
    --table_name=yellow_taxi_trips \ 
    --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"


docker build -t taxi_ingest:v001 .

docker run -it 
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
      --password=root \
      --host=pg-database \
      --port=5432 \ 
      --db=ny_taxi \ 
      --table_name=yellow_taxi_trips \ 
      --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"


    docker run -it taxi_ingest:v001 --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"