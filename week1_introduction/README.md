# Week 1 - Course Notes

The course will teach us the fundamentals of data engineering with modern data technologies such as Google Cloud Platform (GCS & Bigquery), Terraform, Docker, Airflow, dbt, Kafka and Spark. It will teach us the basics of how to design and develop systems for collecting, storing, transforming and analyzing data as per this <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp#architecture-diagram" target="_blank">architecture</a>. Throughout the course, we will use <a href="https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page" target="_blank">New York's Taxi and Limousine Corporation's Trip Records Dataset</a>.

For additional learning, the tutors recommend this book: <a href="https://www.amazon.com/Designing-Data-Intensive-Applications-Reliable-Maintainable/dp/1449373321">Designing Data-Intensive Application</a>. Another book that I would recommend is <a href="https://www.amazon.com/Fundamentals-Data-Engineering-Robust-Systems/dp/1098108302">Fundamentals of Data Engineering</a>.

This week's course will cover the introduction and basic setups. We will be introduced to Docker, Postgres, Google Cloud Platform and Terraform.

## Docker & Postgres


### Docker concepts

Docker is a software platform designed to make it easier for us to create, deploy, and run applications by using containers.

There are 3 core concepts that we need to understand about docker, they are:
- Images. Docker images are like the blueprints for containers and include every single thing that our application need to run. They contain things such as runtime environment, application code and commands for running an application. But they don't actually have them running inside them, they just kind of of store them inside. Docker images are read only, which means that once you've created an image it can't be changed and if you need to change something about the image then you need to instead create a brand new image to incorporate that change.

- Containers. Docker containers are runnable instances of docker images. A container is a process which runs our application as outlined by the image we created. A container is an isolated process, meaning they run independently from any other process on your computer so it's a bit like our application being run in its own box somewhere on our computer, package away with everything it needs to run inside it and it's completely isolated from any other processes on our computer.

- Dockerfile. Dockerfile is the source code of the images. It is a text file with a bunch of instructions to create and build an image.

The course has introduced us with some docker commands:
- `docker build -t`. It is a command used to build an image, as stated in a Dockerfile.
- `docker run -it`. It is used to create and run a container from the given image.
- `docker network create`. It allows us to establish a communication between containers that situated in different network.
- `docker-compose up -d`. This command is used to run the containers. The key difference between `docker run` and `docker-compose up -d` is that `docker run` is entirely command line based, while docker-compose reads configuration data from a YAML file. The second major difference is that docker run can only start one container at a time, while docker-compose will configure and run multiple.
- `docker-compose down`. It is used to shut down containers and remove containers, networks, volumes and images create by `docker-compose up`.
- `docker network ls`. Prints all running network.
- `docker ps -a`. Shows the list of running containers
- `docker stop`. To stop a running container.
- `docker rm`. It is used to remove the containers.

For more, you can visit <a href="https://docs.docker.com/engine/reference/run/" target="_blank">docker CLI reference</a>. For installing docker, please visit the <a href="https://docs.docker.com/get-docker/" target="_blank">Get Docker</a> guide.


### Ingesting Data to Postgres

Before we start data ingestion, lets install `pgcli` first with `pip/conda install pgcli`. `pgcli` is a python package for Postgres command line interface. Then, create a folder `ny_taxi_postgres_data` that Postgres will use to store data. Next, run the following command to run a containerized version of Postgres.

        docker run -it \
            -e POSTGRES_USER="root" \
            -e POSTGRES_PASSWORD="root" \
            -e POSTGRES_DB="ny_taxi" \
            -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
            -p 5431:5432 \
            postgres:13

- `POSTGRES_USER=` is the username of our Postgres database. 
- `POSTGRES_PASSWORD=` is the password for our Postgres database. 
- `POSTGRES_DB=` is the name we give to our database. 
- `-v` is the external volume path directory. 
- `-p` is to map Postgres port on our host machine to the one in our container. In my case I used `5431` because port `5432` already in use.
- `postgress:13` is the image name and tag of Postgress version 13. If the image is not present on our local system, then docker will pull it from the registry.

Once the container runs, we can try to interact with our Postgres database with `pgcli` with the following commands `pgcli -h localhost -p 5431 -u root -d ny_taxi`. 
- `-h` is the host. 
- `p` is the port. 
- `-u` is the username. 
- `d` is the database name.

To start data ingestion, let's create a Jupyter Notebook and download the January 2021 Yellow Taxi Trip Records with `wget  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet`. Then, we need establish a connection with our Postgres database with `create_engine("postgresql://root:root@localhost:5431/ny_taxi")`. Once all set, we first need to `CREATE TABLE` named `yellow_taxi_data` and start storing the data by chuncks to our Posgress database. Please visit this <a href="https://github.com/Balurc/data_eng_zoomcamp/blob/main/week1_introduction/2_docker_sql/upload_data.ipynb" target="_blank">notebook</a> for the full implementation. 

In addition to `pgcli` we can also use `pgAdmin`, a web-based GUI, to interact with our Postgres database more conveniently. We can also run `pgAdmin` in a docker container but we need to ensure both are run in the same network, so that we can access our postgres database with `pgAdmin`. For this, we need to create a docker network `docker network create pg-network`. `pg-network` is the network name that we will create. Once done, we update our commands before with 2 new instructions, `--network=` for the network name and `--name` as our container name. Next, we re-run our Postgress container with the updated command below.

        docker run -it \
            -e POSTGRES_USER="root" \
            -e POSTGRES_PASSWORD="root" \
            -e POSTGRES_DB="ny_taxi" \
            -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
            -p 5431:5432 \
            --network=pg-network \
            --name pg-database \
            postgres:13

And next we open new terminal and run the `pgAdmin` container. And after this we will have 2 containers running on 2 separate terminals.

        docker run -it \
            -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
            -e PGADMIN_DEFAULT_PASSWORD="root" \
            -p 8080:80 \
            --network=pg-network \
            --name pgadmin \
            dpage/pgadmin4

- `PGADMIN_DEFAULT_EMAIL=` is the email for `pgAdmin` sign in. 
- `PGADMIN_DEFAULT_PASSWORD=` is password for `pgAdmin`. 
- `-p` is the port to map `pgAdmin` to our host machine. 
- `dpage/pgadmin4` is the image name.

To start interacting with our database through `pgAdmin`, go to `localhost:8080` in our browser and log in to `pgAdmin` with above details (email & password). Once logged in, create a new server and specify `pg-database` as the host name to connect with our  NYC TLC data.


### Dockerizing the Ingestion Script

To level up our data ingestion process, lets convert our previous ingestion steps (Jupyter Notebook) to a script and command that accept arguments for inputs such as username, password, host, port, table name, url, etc. For full code implementasion you can check `ingest_data.py`.

To test the script, in `pgAdmin` we first need to `DROP TABLE yellow_taxi_data;` then run below command.

        python ingest_data.py \
            --user=root \
            --password=root \
            --host=localhost \
            --port=5432 \
            --db=ny_taxi \
            --table_name=yellow_taxi_trips \
            --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"


Once done, go to `pgAdmin` again and check if the data is already inside our Postgres database with the following query:

        select count(1)
        from yellow_taxi_trips;

To dockerize out script, we first need to include `ingest_data.py` and the libraries needed to run it in our `Dockerfile`. 

        FROM python:3.9

        RUN apt-get install wget
        RUN pip install pandas sqlalchemy psycopg2 pyarrow

        WORKDIR /app
        COPY ingest_data.py ingest_data.py

        ENTRYPOINT ["python", "ingest_data.py"]

Then create a new image with `docker build -t taxi_ingest:v001`. `taxi_ingest` is our image name with tag (`-t`) `v001`. Once we built the image, we re-run our `postgres` container with the following command.

        docker run -it  \
            --network=pg-network \
            taxi_ingest:v001 \
                --user=root \
                --password=root \
                --host=pg-database-2 \
                --port=5432 \
                --db=ny_taxi \
                --table_name_taxi=yellow_taxi_trips \
                --table_name_zones=taxi_zones \
                --url_taxi="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet" \
                --url_zones="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

Running the above script will give us 2 tables `yellow_taxi_trips` and `taxi_zones` (zone IDs for pick ups and drop offs). I ingested taxi zones data for sql refresher section and homework purpose. Next, we can go to `pgAdmin` and check if the data is already in out Postgres database with this query:
        select count(1)
        from taxi_zones;

Next, we can further enhance or simplify our data ingestion process with `docker-compose`. The current script requires us to:
- Run 2 containers separately in two terminals.
- Specify a network so that both containers can interact with each other.
- Specify long parameters on the `docker run` commands.

But with `docker-compose` we dont have to do any of the above. With `docker-compose` we dont need to specify the network because `docker-compose` will take care of the communication between both containers (will run in the same network), hence we wont need 2 terminals to run 2 containers. `docker-compose` uses YAML files to store all configurations, hence we can store the all the parameters we specify in `docker run` command in this file.

        services:
            pgdatabase:
                image: postgres:13
                environment:
                    - POSTGRES_USER=root
                    - POSTGRES_PASSWORD=root
                    - POSTGRES_DB=ny_taxi
                volumes:
                    - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
                ports:
                    - "5431:5432"
            pgadmin:
                image: dpage/pgadmin4
                environment:
                    - PGADMIN_DEFAULT_EMAIL=admin@admin.com
                    - PGADMIN_DEFAULT_PASSWORD=root
                ports:
                    - "8080:80"

Before running the file, you first need to ensure all other containers have been shutdown.
Use `docker ps -a` to show the containers that actively running now and to stop them, use `docker stop container_id`. Then you can run `docker-compose up -d` command within the same directory where `docker-compose.yaml` file exists. To shutdown the containers you can use `CTRL+C` or `docker-compose down`.


### SQL Refresher

This section will use 2 tables, `yellow_taxi_trips` and `taxi_zones` (zone IDs for pick ups and drop offs).

Count all rows in the `taxi_zones` table.

        select count(1)
        from taxi_zones;
    
Grab the first 100 rows in the `yellow_taxi_trips` table.

        select *
        from yellow_taxi_trips
        limit 100;

Grab the first 100 rows in the `yellow_taxi_trips` table, give aliases to table names and replace the IDs of `PULocationID` and `DOLocationID` with actual zone IDs for pick ups and drop offs.

        select *
        from
          yellow_taxi_trips t,
          taxi_zones zpu,
          taxi_zones zdo
        where
          t."PULocationID" = zpu."LocationID" and
          t."DOLocationID" = zdo."LocationID"
        limit 100;

Grab the first 100 rows in the `yellow_taxi_trips` table and use implicit joins to show `Borough` and `Zone` (combined and separated with /) for both `pickup_loc` and `dropoff_loc`.

        select
          tpep_pickup_datetime,
          tpep_dropoff_datetime,
          total_amount,
          concat(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
          concat(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
        from
            yellow_taxi_trips t,
            taxi_zones zpu,
            taxi_zones zdo
        where
            t."PULocationID" = zpu."LocationID" and
            t."DOLocationID" = zdo."LocationID"
        limit 100;


Rewrite the above query with explicit inner joins (join or inner join).

        select
          tpep_pickup_datetime,
          tpep_dropoff_datetime,
          total_amount,
          concat(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
          concat(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
        from
            yellow_taxi_trips t join taxi_zones zpu
              on t."PULocationID" = zpu."LocationID"
            join taxi_zones zdo
              on t."DOLocationID" = zdo."LocationID"
        limit 100;


Grab the first 100 rows in the `yellow_taxi_trips` table and whose pick up location is null.

        select
          tpep_pickup_datetime,
          tpep_dropoff_datetime,
          total_amount,
          "PULocationID",
          "DOLocationID"
        from yellow_taxi_trips t
        where "PULocationID" is NULL
        limit 100;


Grab the first 100 rows in the `yellow_taxi_trips` table and whose location ID not in `taxi_zones` table.

        select
          tpep_pickup_datetime,
          tpep_dropoff_datetime,
          total_amount,
          "PULocationID",
          "DOLocationID"
        from yellow_taxi_trips t
        where "DOLocationID" not in (select "LocationID" from taxi_zones)
        limit 100;

Delete rows in `taxi_zones` table with `LocationID` 142.

        delete from taxi_zones where "LocationID" = 142;


Using `Left Join` to show all rows from the "left" table but only the rows from the "right" table that overlap with the "left" table. 

          select
              tpep_pickup_datetime,
              tpep_dropoff_datetime,
              total_amount,
              concat(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
              concat(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
          from
              yellow_taxi_trips t left join taxi_zones zpu
                  on t."PULocationID" = zpu."LocationID"
              left join taxi_zones zdo
                  on t."DOLocationID" = zdo."LocationID"
          limit 100;

Grab the first 100 rows in the `yellow_taxi_trips` table and use `date_trunc` to trunctates a timestamp. `day` parameter will convert any timestamp (hours, minutes, seconds) into 00:00:00.

          select
              tpep_pickup_datetime,
              tpep_dropoff_datetime,
              date_trunc('day', tpep_pickup_datetime),
              total_amount,
          from yellow_taxi_trips t
          limit 100;


Similar to previous query, but this time it casts `timestamp` data type into `date`. The hours, minutes and seconds will be ommitted.

          select
              tpep_pickup_datetime,
              tpep_dropoff_datetime,
              cast(tpep_pickup_datetime as date) as "day",
              total_amount,
          from yellow_taxi_trips t
          limit 100;

Counts the amount of records in the `yellow_taxi_trips` table grouped by day, show maximum `total_amount` and `passenger_count` and ordered by descending order of total count (records). 

          select
              cast(tpep_pickup_datetime as date) as "day",
              count(1) as "count",
              max(total_amount),
              max(passenger_count)
          from yellow_taxi_trips t
          group by cast(tpep_pickup_datetime as date)
          order by "count" desc;


Pretty similar with the previous query, but now its grouped by (aggregation level) day and  `DOLocationID`. In addition to that, we use relative reference in the group by statement, 1 for `cast(tpep_pickup_datetime as date)` and 2 for `DOLocationID`.

          select
              cast(tpep_pickup_datetime as date) as "day",
              "DOLocationID",
              count(1) as "count",
              max(total_amount),
              max(passenger_count)
          from yellow_taxi_trips t
          group by 1, 2
          order by "count" desc;


Similar to previous query, but this time we order the results by day and `DOLocationID` in ascending order.
          select
              cast(tpep_pickup_datetime as date) as "day",
              "DOLocationID",
              count(1) as "count",
              max(total_amount),
              max(passenger_count)
          from yellow_taxi_trips t
          group by 1, 2
          order by
              "day" asc,
              "DOLocationID" asc;


For more on learning SQL, I would recommend these 2 books:
- <a href="https://www.amazon.com/SQL-Intermediate-Programming-Practical-Exercises/dp/B0B5MM3RMK" target="_blank">SQL: 3 books 1 - The Ultimate Beginner, Intermediate & Expert Guides To Master SQL Programming Quickly with Practical Exercises</a>.
- <a href="https://www.amazon.com/SQL-Data-Analysis-Techniques-Transforming/dp/1492088781" target="_blank">SQL for Data Analysis: : Advanced Techniques for Transforming Data into Insights</a>.


        

## Google Cloud Platform (GCP) & Terraform 

### GCP Setup

During this course we will be using GCP and we will utilize Google Cloud Storage (GCS) as <a href="https://cloud.google.com/learn/what-is-a-data-lake" target="_blank">Data Lake</a> and Google BigQuery as our <a href="https://cloud.google.com/learn/what-is-a-data-warehouse" target="_blank">Data Warehouse</a>.

You can follow these initial steps to setup your GCP account:
- To get started, you need to have a <a href="https://cloud.google.com/free" target="_blank">GCP account</a> and you can set it up for free and get $300 credit.
- Next, create a new project and name it `dtc-de` (example). Once created, switch to the project that we just created.
- Create a service account by going to `IAM & Admin > Service accounts > Create service account`. Provide a name to the service account (`dtc-de-user`), then click CREATE AND CONTINUE. Then, select user Viewer role for now and click CONTINUE. Next, click on DONE as there is no need to grant users access to this service account for now.
- Once created, you now need to generate and download the authentication key in JSON. Go to Manage Keys, then `ADD KEY > CREATE NEW KEY`. Select JSON and click CREATE, then the a file will be downloader to your local computer.
- Install Google SDK by following <a href="https://cloud.google.com/sdk/docs/install" target="_blank">this instruction</a>. Use this command `gcloud -v` to check whether Google SDK is already installed on your machine.
- Set your environment variable google applications credentials to the downloaded service account keys `export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json`. Then, refresh the token and verify the authentication with Google SDK `gcloud auth application-default login`. 

Next we will create 2 resources in GCP environment, one for our Data Lake (Google Cloud Storage) and the other for Data Warehouse (Google BigQuery). Follow these steps:
- Go to IAM & Admin > IAM and select the service acount that we previously created (`dtc-de-user`). Click Edit principal (pencil shaped icon) and assign the following roles Storage Admin, Storage Object Admin and BigQuery Admin to the service account. Once done, click SAVE.
- Enable the APIs for the project to interact with GCP:
  - https://console.cloud.google.com/apis/library/iam.googleapis.com
  - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
- Please ensure `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set. `export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json`.


### Terraform Basics & Installation

In this course we will user <a href="https://www.terraform.io/use-cases/infrastructure-as-code" target="_blank">Terraform</a> to provision our infrastructure resources as code. Terraform is an open source infrastructure as a code tool. 
- It allows us to define both on-premises and cloud resources in human-readable configuration files that can be easily versioned, reused, and shared. So we can so think of this as a git version control but for infrastructure. 
- It also allows us to bypass the cloud vendor GUIs.

To install Terrafom you can go <a href="https://www.terraform.io/downloads" target="_blank">here</a> and choose the one's that matched your OS. For macOS users you can simply install Terraform with the following commands:

        brew tap hashicorp/tap
        brew install hashicorp/tap/terraform


Next, create a folder named `terraform` with 3 files inside of it; `main.tf`, `variables.tf` and `.terraform-version`. Then you can make sure if Terraform already installed and what version it has by `terraform version` command.

`main.tf` contains the basic infrastructure written in Terraform language. As you can see, we have specified 2 resources for out Data Lake and Data Warehouse. At minimum, there are 3 main blocks here; `terraform`, `provider` and `resource` block. We should only have 1 `terraform` block, but we can have multiple `provider` and `resource` blocks.

- `terraform` block contains the settings of Terraform.
- `provider` block configures a specific provider that we need to interact with (cloud providers, api, etc).
- `resource` block defines the actual components of our infrastructure.

`variable.tf` contains the variables that are used to define our configurations. Terraform variables is saved independently from the deployment plans, which makes the values easy to read and edit from a single file. 

Some important Terraform commands that we need to be familiar with:
- `terraform init` to initialize & configure the backend, installs plugins/providers, & checks out an existing configuration from a version control. This will initialize a state file (.tfstate).
- `terraform plan` to match/preview local changes against a remote state, and proposes an execution plan.
- `terraform apply` to ask for approval to the proposed plan, and applies changes to cloud.
- `terraform destroy` to remove your stack from the cloud.  

### Creating Infrastructure with Terraform

Before creating the infrastructure, we first need to update `main.tf` that has 2 `resource` blocks, one for our Data Lake (Google Cloud Storage) and the other for Data Warehouse (Google BigQuery) as below:

    terraform {
      required_version = ">= 1.0"
      backend "local" {}
      required_providers {
        google = {
          source  = "hashicorp/google"
        }
      }
    }

    provider "google" {
      project = var.project
      region = var.region
      // credentials = file(var.credentials)
    }

    # Data Lake Bucket
    resource "google_storage_bucket" "data-lake-bucket" {
      name          = "${local.data_lake_bucket}_${var.project}" 
      location      = var.region

      storage_class = var.storage_class
      uniform_bucket_level_access = true

      versioning {
        enabled     = true
      }

      lifecycle_rule {
        action {
          type = "Delete"
        }
        condition {
          age = 30  // days
        }
      }

      force_destroy = true
    }

    # Data Warehouse
    resource "google_bigquery_dataset" "dataset" {
      dataset_id = var.BQ_DATASET
      project    = var.project
      location   = var.region
    }

For `variables.tf`, you need to ensure that the region selected is near to you.

    variable "region" {
      description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
      default = "asia-southeast1"
      type = string
    }

To start, `terraform init` command will help us to initialize our configuration. This will download the necessary plugins to connect to GCP and download them to ./.terraform. Once ready, you can use `terraform apply` command to create the resources (it will ask your approval and your project ID). You can also use `terraform plan` to review your execution plan (not going to create any resources). After you have successfully created the infrastructure, you can destroy it with `terraform destroy` (to ensure it doesn't consume any more credit).

For more on how to use Terraform with GCP, you can visit <a href="https://learn.hashicorp.com/collections/terraform/gcp-get-started" target="_blank">this link</a>



## Setting up the Environment on Google Cloud (Cloud VM + SSH access)

- In GCP, go to Compute Instances > VM Instance. Enable the Compute Engine API if prompted.
- Generate SSH Keys. This is the key that we will use to log into the VM instance. First, you need to create a `.ssh` directory (if you don't have one already) with `mkdir ~/.ssh`. In you ssh directory run this command to create a new SSH key pair `ssh-keygen -t rsa -f gcp -C <yourname> -b 2048` (check <a href="https://cloud.google.com/compute/docs/connect/create-ssh-keys" target="_blank">this link</a>). You can leave the passphrase (password) empty for now. Once executed, you will have 2 keys; `gcp` and `gcp.pub`. As a word of caution, never share your private key to anyone.
- Upload public key (`gcp.pub`) to Google Cloud.  Under Compute Engine go to Metadata select SSH tab, and add SSH key. Then enter the key found in `gcp.pub` file and click save.
- Create VM instance. Click CREATE INSTANCE -> give a name (`de-zoomcamp`) -> select a region (`asia-southeast1`) -> select machine type (`e2-standard-4`) -> go to Boot Disk and select `Ubuntu`, `Ubuntu 20.04 LTS` and size `30GB` -> click SELECT -> scroll down and click CREATE.
- SSH into VM. Copy the External IP, go to terminal and run this command `ssh -i ~/.ssh/gcp <yourname>@<externalip>`. Now that we are connect to the VM, let's check what kind of machine we get with `htop`. Press `CTRL + C` to exit.
- Configure VM and setup local ~/.ssh/config. First we will download Anaconda with `wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh` and run `bash Anaconda3-2022.05-Linux-x86_64.sh` to install. Once done, next we create a config file with `touch config` and write as below. Once saved, you can enter your VM with `ssh de-zoomcamp`. If you want to log out, use `CTRL + D`.

        Host de-zoomcamp
          HostName <externalip>
          User <yourname>
          IdentityFile ~/.ssh/gcp
- Install docker with `sudo apt-get update` and `sudo apt-get install docker.io`.
- SSH with VS Code. Open your VS code and install `Remote - SSH` extension. Then from command palette search and select Connect to Host and select de-zoomcamp (this appears because we created the config file earlier).
- `git clone` the entire data engineering zoomcamp github repo to the VM, `git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git`.
- At this stage if you have permission issues (denied) when running `docker run`, you need to grant permission with this commands `sudo groupadd docker`, `sudo gpasswd -a $USER docker` and `sudo service docker restart`. Once completed, we need to log out and log back in so that group membership is re-evaulated.
- Next, we install `docker-compose`. First, create (`mkdir`) folder called bin in your VM, change directories into it, and run `wget https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -O docker-compose`. Next run this command `chmod +x docker-compose` to let the system knows that this is an executable file. Now, we need to make it visible to every directory. In the VM home folder, run `nano .bashrc` and add bin directory to our path by `export PATH="${HOME}/bin:${PATH}"`. Save and exit and run `source .bashrc` to refresh the changes. Now try to go to `2_docker_sql` directory and running `docker-compose up`. If you want to stop the docker containers, use `docker-compose down`, but for now let's keep them running.
- Install `pgcli`. Now head to home directory and install `pgcli` with Anaconda `conda install -c conda-forge pgcli`. To test the connection to your postgres database, run this command `pgcli -h localhost -U root -p 5431 -d ny_taxi`.
- Setup port forwarding to local machine. In your VS code, open `PORTS` tab and click forward the port (for postgres database). Now click add port and add `8080` port for `pgAdmin`. And for Jupyter Notebook, add `8888` port on the port tab. Then use the link show in the terminal to access Jupyter Notebook from our local machine.
- Installing Terraform. Navigate to bin directory and run `wget https://releases.hashicorp.com/terraform/1.1.4/terraform_1.1.4_linux_amd64.zip`. To unzip the file we first need to install `unzip` with `sudo apt-get install unzip`, then `unzip <zipfile>` and remove the zip file with `rm *.zip`.
- SFTP Google credentials to VM. We need to copy the JSON file containing our authentication key to our VM. To connect with our VM, we'll run this command `sftp de-zoomcamp` from our local machine. Once connected, create a new directory `mkdir .gc` then put the JSON file in the directory `put <lpathtoJSON>`. Next we need to activate our service account credentials by running 2 commands; `export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/<service-account-authkeys>.json` and `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS`.
- Run Terraform. Now we can run the Terraform commands from before `terraform init`, `terraform plan` and `terraform apply`. 
- Shutdown VM. To shutdown the VM from terminal use `sudo shudown now`. We can also do this from GCP GUI. And, to run the VM again use this command `ssh de-zoomcamp` but don't forget to update the HostNamet with the latest external ip in `~/ssh/config` file.



## Homework Solution (Question 3 - 6)

Question 3: How many taxi trips were there on January 15? Consider only trips that started on January 15.

    select count(*)
    from yellow_taxi_trips
    where tpep_pickup_datetime::date = '2021-0-15'

Question 4: Find the largest tip for each day. On which day it was the largest tip in January? Use the pick up time for your calculations.

    select cast(tpep_pickup_datetime as date) as day,
      max(tip_amount) as max_tip
    from yellow_taxi_trips
    group by day
    order by max_tip desc
    limit 1;

Question 5: What was the most popular destination for passengers picked up in central park on January 14? Use the pick up time for your calculations. Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown".

    select coalesce(do.zone,'Unknown') as zone,
          count(*) as count_trips
    from yellow_taxi_trips as t
      inner join taxi_zones.taxi_zone_lookup as pu on t.pulocationid = pu.locationid
      left join taxi_zones.taxi_zone_lookup as do on t.dolocationid = do.locationid
    where pu.zone ilike '%central park%'
      and tpep_pickup_datetime::date = '2021-01-14'
    group by zone
    order by count_trips desc
    limit 1;

Question 6: What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? Enter two zone names separated by a slash. For example: "Jamaica Bay / Clinton East".

    select concat(coalesce(puzones.zone,'Unknown'), '/', coalesce(dozones.zone,'Unknown')) as pickup_dropoff,
      avg(total_amount) as avg_price_ride
    from trips as yellow_taxi_trips as t
      left join taxi_zones as pu on t.pulocationid = pu.locationid
      left join taxi_zones as do on t.dolocationid = do.locationid
    group by pickup_dropoff
    order by avg_price_ride desc
    limit 1;
