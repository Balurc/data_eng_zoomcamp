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
