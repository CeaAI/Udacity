# Data Warehousing with Redshift
---

The purpose of this project is to create a data warehouse for a startup Sparkify and build an ETL (Extract,Transform,Load) process moving data from s3 to redshift

## Schema Design

This project uses the star schema in redshift. It has 1 fact table and 4 dimensions table. The data is thus denormalized. The star schema also allows for fast aggregations. Thus the database is optimized for reads. The diagram below shows the nature of the star schema for this project

![starschema](sparkify.png)

## ETL PIPELINE

The queries for creating, dropping and selecting information for the db can be found as function in the sql_queries.py. The create_tables.py uses the create table queries found in sql_query.py to create (create_tables), or to drop tables (drop_tables) as neccessary in redshift.

ETL processes found in the etl.py is used to populate songs and artists tables from the  s3 bucket first by copying the informstion from s3 using the COPY command (extract). The information extracted is then transformed (transform) into what the Analytics team would need using the star schema and loaded into the tables created in the redshift cluster (load).


## Installation
### Create a redshift cluster on AWS:
 - Create an IAM role by going to AWS IAM, if you have an AWS account or signup for AWS , click the roles options, creating new IAM role
 - Create a redshift cluster on AWS   and create a redshift cluster using the redshift free trial. 
 - Create subnet group if you do not have one from the redshift config options
 - Add Admin user name and Password
 - Copy the folloing information fro the redshift cluster when it has been created and replace the sample below with the information in the dwh.cfg file :
   ```
   HOST=redshift-cluster-1.cmxv0gi5gfqp.us-west-2.redshift.amazonaws.com
   DB_NAME=dev 
   DB_USER=ceip  #admn username
   DB_PASSWORD= ****** # adminpassword
   DB_PORT=5439 #port usually 5439
   
   [IAM_ROLE]
   ARN=arn:aws:iam::999488985203:role/admin_role  #click on cluster permission on redshift and copy arn
   REGION='us-west-2' # get from aws region or check the redshift host to see what region your redshift is hosted
   ```
- Ensure your s3 bucket is in the same region as your redshift


### Run the files
- Run the create_tables.py files first to create tables in redshift
- Run etl.py to carryout extract, transform and load

```
 python create_tables.py
 
 python etl.py
```

## Debugging ETL Issues

 You can debug etl issues on the redshift UI on AWS. Click the editor and you will see whre to run redshift queries.
 - Connect using a new connection which creates a temporary connection and run the follwoing query
 
 ```
 select * from stl_load_errors 
 ```
 
 - To debug the problem with a specific process
  - Go to the redshift cluster running the process , click on query monitoring, there  you will see the 
###


