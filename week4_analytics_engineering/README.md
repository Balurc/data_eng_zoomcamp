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

