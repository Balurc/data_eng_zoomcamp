# WEEK 4 - Course Notes

This week's course will discuss analytics engineering, you can find the repo  <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering" target="_blank">here</a> and the accompanying course videos <a href="https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=36" target="_blank">here</a>.

This notes will cover:
- Basics of Analytics Engineering
- dbt (Data Build Tool)

## Basics of Analytics Engineering

The recent development in data domain/technologies have rapidly changed the way the data teams working. . This development have introduced us with some new "modern data stack" tools that able to help organzation to save time, effort and money. 
- Massive parrallel processing (MPP) databases, such as BigQuery, Redshift, Snowflake, etc.
- Data-pipelines-as-a-service, simplify the ETL process with tools such as Fivetran & Stitch.
- SQL first and version control systems tool such as Looker.
- Self service analytics tool, such as Mode.
- Data governance.

This development has also changed the way stakeholders consume the data, and left a gap in the roles we had in the data team. Previously we have:
- Data engineer prepares and maintain the infrastructure that the data team will need.
- Data analyst/scientist utilizes data hosted in the infrastruture to answer questions, solve business problems and create prediction models (data scientist).

With these new tools inplace, there are 2 trends we can see nowdays that has left a gap:
- Data analysts and data scientists that write more and more code even though they are not meant to be software engineers and haven't been trained for it. 
- And also data engineers who have good software engineering skills but they are not trained for answering and solving business problems.

This is the gap that the <a href="https://www.getdbt.com/what-is-analytics-engineering/" target="_blank">analytics engineer</a> is trying to fill. It introduces good software engineering practices to the efforts of data analysts and data scientists.
Some tools that an analytics engineer might be exposed to:
- Data loading (Fivetran, Stitch, etc).
- Data storing (BigQuery, Snowflake, etc).
- Data modelling (dbt, Dataform, etc).
- Data presentation (Tableau, Looker, etc).

### Data Modelling Concepts

![](images/data_modelling.png)

Let's recap some of some of data modelling concepts on ETL and ELT that we have covered in previous lessons.
- In ETL, we are going to extract data from the sources and transform it and then load it to a data warehouse. This approach will take longer to implement because we first have to transform the data, but this also means that we are going to have more stable and compliant data because it's clean.
- In ELT, we are going to transform the data once it is in the data warehouse. This approach is faster and more flexible to implement than ETL's   because all the data will be loaded and transformed in the same data warehouse.

### Dimensional Modelling

The objectives of dimensional modelling (Ralph Kimball) are:
- To deliver data that business users can easily understand.
- To deliver fast query performance.

Other approaches of dimensional data modelling are <a href="https://www.astera.com/type/blog/data-warehouse-concepts/" target="_blank">Bill Inmon's</a>  and <a href="https://en.wikipedia.org/wiki/Data_vault_modeling" target="_blank">data vault</a>.

### Elements of Dimensional Modelling

The dimensional modelling concept is mainly about two types of table; fact tables and dimensional tables, also known as <a href="https://en.wikipedia.org/wiki/Star_schema" target="_blank">Star Schema modelling</a>.

Fact tables are:
- One that holds information on some quantitative metrics (i.e. measurements), over which some sort of calculation can be performed. Some common examples of facts tables include orders, logs and time-series financial data. Some examples of fact tables are orders and sales.
- Contain facts about a business process.
- Can be thought of as "verbs".

Dimension tables are:
- Provide context to fact tables.
- Correspond to business identity.
- Can be thought of as "nouns".

An analogy that we use to think and describe about Kimball's dimensional modelling is the restaurant analogy. In this analogy we have 3 areas:
- Stage area - the kitchen storage. The area that contains raw data and not meant to exposed to everyone.
- Processing area - the kitchen. The area where we process the raw data into data models in efficient manners and follow the agreed standards. This area is limited to people who knows how to do data modelling (which in the restaurant analogy, only the cook).
- Presentation area - the dining hall. The area where we have the exposure to business users and stakeholders and present our data.


## dbt (Data Build Tool)

### What is dbt?

dbt stands for data built tool. dbt helps with the transformation process of our data so that we can expose them to the business users in the form of reports or vizualizations.

dbt allows us to transform raw data with SQL following software engineering best practices like modularity, portability, model deployment (with version control & CI/CD) and testing & documentation.

### How dbt works?

![](images/dbt_works.png)

It works by adding model layers to the data warehouse and the purpose of these models is to transform the raw data into tables or views. The transformed data will later be persited back to the data warehouse. A model layer is a SQL file that contains a set of `SELECT` instructions (DQL) with no DDL or DML instructions. dbt will compile the code by creating the DDL or DML file and push it to the data warehouse.

### How to use dbt?

dbt has 2 products:

![](images/dbt_products.png)

The fundamental difference between dbt Core and dbt Cloud is the way we work with each. To work with dbt Core we will use command-line interface (CLI), and for dbt Cloud we will use it's integrated development environment (IDE). 

[](images/to_do.png)

In this week's course, we are going to transform the raw data and create data models with dbt Cloud. We are also going to do the deployment and orchestration in dbt Cloud. If you want to develop the data models with postgres database, you can do that with dbt Core and running the models through the CLI. 

### Intial Setup

To begin, creates 2 new datasets in your BigQuery and a repository from scratch:
- 1 datasets for developing evironmrnt, to create the data models.
- 1 datasets for production, to run the models after deployment.
- Create a new repository from scratch, here as a <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering" target="_blank">reference</a>.

### dbt Cloud Setup with BigQuery

Follow these steps to setup your dbt Cloud with BigQuery and additionally, you can follow the detailed guides <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md" target="_blank">here</a>.

1. Create a BigQuery Service Account.
    - Go <a href="https://console.cloud.google.com/apis/credentials/wizard" target="_blank">here</a> and create a service account. Select `BigQuery Admin` role, click `CONTINUE` and `DONE`.
    - Next, go to the service account that you just create and click on `KEYS` tab, then click `ADD KEY` and select `CREATE NEW KEY`, pick `JSON` key type and click `CREATE` to create and download your service account key.

2. Create a dbt Cloud Project.
    - Go <a href="https://www.getdbt.com/pricing/" target="_blank">here</a> and create free dbt developer account. Once logged in, you will be prompt to create a new project.
    - Next, click on `BEGIN` and select `BigQuery` as your database connection.
    - Upload the downloaded service account key on the `create from file` option. 
    - Next, scroll down to the end of the page and in the `Development Credentials` section put the dataset name in the BigQuery where you are going to develop you dbt models.
    - To finalize, click on `Test` and continue with the setup.

3. Add Github Repository.
    - Go to your GitHub account, select git clone and paste the SSH key from your repo.
    - Once you get the deploy key, go to your GitHub repo again and go to the setting tabs. Under security select deploy keys, then click on `add deploy key` and paste the deploy key and tick on "write access" and click `Add Key`.

If you want to use dbt Core instead, follow these guides:
- Guide to install <a href="https://docs.getdbt.com/dbt-cli/install/overview" target="_blank">dbt Core</a>.
- Setup  <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering/docker_setup" target="_blank">dbt with BigQuery on Docker</a>.


### Development of dbt Models

#### Anatomy of a dbt model

Before we start creating the data models, let's first have look at the structure and elements that we are going to see in dbt:

- `Jinja` in dbt. <a href="https://jinja.palletsprojects.com/en/3.1.x/templates/" target="_blank">Jinja</a> for SQL is a template language for SQL statements and script. Jinja statement starts and ends with 2 curly brackets `{{ sql_statement_here }}`.

- `Macros`. <a href="https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros#macros">Macros</a> in Jinja are pieces of code that can be reused multiple times – they are analogous to "functions" in other programming languages, and are extremely useful if you find yourself repeating code across multiple models.

- By default, dbt provides 4 materialization strategies:
  - Table - drop the table if it already exists in our data warehouse and create the table in the schema that we are working with.
  - View - is exactly the same as table but here it would be something like `CREATE` or `ALTER`.
  - Incremental - run our model and transform and insert latest data only to our table, useful for data that doesn't really change everyday.
  - Ephemeral - creates `CTE` that separated in another file.

#### The FROM clause

There are 2 sources in dbt that can be defined from the `FROM` clause:
- <a href="https://docs.getdbt.com/docs/building-a-dbt-project/using-sources">Sources</a>; the data loaded to our data warehoouse that we use as sources for our data models.
  - We use `source()` function and the configuration is defined in the yml files (model's folder).
  - Used with source macro that will resolve the name to the right schema, plus build the dependencies automatically.
  - Source freshness can be defined and tested, which can be useful to check whether our data pipelines are working properly.

An example of how to declare a source in a yml file:

    sources:
        - name: staging
          database: production
          schema: trips_data_all

          loaded_at_field: record_loaded_at
          tables:
            - name: green_tripdata
            - name: yellow_tripdata
              freshness:
                error_after: {count: 6, period: hour}

An example of how to reference source in a SQL statement under `FROM` clause: 

    FROM {{ source('staging','yellow_tripdata') }}

- <a href="https://docs.getdbt.com/docs/building-a-dbt-project/seeds">Seeds</a>; csv files stored under `seeds` folder and can be loaded to our data warehouse.
  - Use `ref()` function to refer to the seed in the model's folder.
  - Run `dbt seed -s filename` to create a table in our data warehouse.
  - Recommended for data that doesn't change frequently.

An example of how to reference seeds in a SQL statement under `FROM` clause: 

    SELECT
        locationid,
        borough,
        zone,
        replace(service_zone, 'Boro', 'Green') as service_zone
    FROM {{ ref('taxi_zone_lookup) }}

#### Define a source and develop models.

Let's create our first model and follow these steps:
1. On your dbt Cloud, create 2 folders under your `models` folder named as `staging` and `core`. And delete the `example` folder.

2. Under staging create a yaml file named `schema.yml` and let's define our sources and tables here. Write below statements and click save.

        version: 2

        sources:
            - name: staging
              database: your_projectid
              schema: trips_data_all

              tables:
                  - name: green_tripdata_external_table
                  - name: yellow_tripdata_external_table

3. Next, create a new SQL file named `sgt_green_trip_data.sql` and let's write something to create our first model as below and click save.
        # We'll create a view
        {{ config(materialized='view') }}

        SELECT
                -- identifiers
            cast(vendorid as integer) as vendorid,
            cast(ratecodeid as integer) as ratecodeid,
            cast(pulocationid as integer) as  pickup_locationid,
            cast(dolocationid as integer) as dropoff_locationid,

            -- timestamps
            cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
            cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

            -- trip info
            store_and_fwd_flag,
            cast(passenger_count as integer) as passenger_count,
            cast(trip_distance as numeric) as trip_distance,
            cast(trip_type as integer) as trip_type,

                -- payment info
            cast(fare_amount as numeric) as fare_amount,
            cast(extra as numeric) as extra,
            cast(mta_tax as numeric) as mta_tax,
            cast(tip_amount as numeric) as tip_amount,
            cast(tolls_amount as numeric) as tolls_amount,
            cast(ehail_fee as numeric) as ehail_fee,
            cast(improvement_surcharge as numeric) as improvement_surcharge,
            cast(total_amount as numeric) as total_amount,
            cast(payment_type as integer) as payment_type,
            cast(congestion_surcharge as numeric) as congestion_surcharge

        FROM {{ source('staging', 'green_tripdata_external_table') }}
        LIMIT 100

4. You can run the model with `dbt run --select your_model_name` or `dbt run -m your_model_name` or `dbt run -f your_model_name`. For more on dbt commands you can visit <a href="https://docs.getdbt.com/reference/dbt-commands">here</a>.

5. Now go to your BigQuery and you should see there is a view created under your development dataset named `stg_green_trip_data`.
  
  
