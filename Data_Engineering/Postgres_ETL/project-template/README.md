# Data Modelling with Postgres
---

The purpose of this project is to create a Postgres database schema for  sparkifydb, the databse been used by a startup Sparkify and build an ETL (Extract,Transform,Load) pipeline for data analysis

## Schema Design

This project uses the star schema. It has 1 fact table and 4 dimensions table. The data is thus denormalized. The star schema also allows for fast aggregations. Thus the database is optimized for reads. The diagram below shows the nature of the star schema for this project

![starschema](sparkify.png)

## ETL PIPELINE

The queries for creating, dropping and selecting information for the db can be found as function in the sql_queries.py. The create_tables.py uses the create table queries found in sql_query.py to create (create_tables), or to drop tables (drop_tables) as neccessary after creating databse using the the create_database function.

ETL processes found in the etl.py is used to populate songs and artists tables from the  data/song_data json file. Also the  data/log_data json files is extracted, transformed and loaded to populate the time and users_table. Four tables are populated using the insert queries in  sql_queries.py.

A SELECT query collects song and artist id from the songs and artists tables and combines this with log file derived data to populate the songplays fact table.


## Installation

### Using Docker Image
- Install Docker:
 If you do not already have docker on your local machine you can install docker through this [link]()

- Download Docker Image:
 A docker image already exists having the postgresql set with user="student" and password ="student". 
 Download the docker image by first logging into your docker machine using the following command on your terminal

 ```
 docker login

 ```
 Then download the docker image
 
 ```
 docker pull onekenken/postgres-student-image

 ```
 Run the container 
 ```
docker run -d --name postgres-student-container -p 5432:5432 onekenken/postgres-student-image

 ```

 This will enable you to connect to postgres on the localhost at port 5432. You can thus use the already define connection in the create_tables.py

 conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")

### Fork and Clone the repository
After forking this repo, on your local machine run 
``` 
git clone  <gitrepo url >

```
### Install Dependencies
if you do not already have the  following packages installed you can install them; psycopg2, pandas, Ipython-SQl. For those using python3 and have pip already install you can install the packages

```
pip3 install psycopg2
pip3 install pandas
pip3 install ipython-sql
```
### Go into the directory
To use this project you have to go into the project-template directory. They you can run either of the following python file

```
 python3 create-tables.py
 python3 etl.py
```