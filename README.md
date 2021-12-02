# Sparkify Amazon Redshift Data Warehouse set up and ETL pipeline

## General info
We work with the data of a fictitious music streaming startup called Sparkify. Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.

Sparkify has grown their user base and song database and want to move their processes and data onto the cloud.

We are building an __ETL pipeline__ which extracts the data from AWS __S3__, __stages them in Amazon Redshift__, and __transforms data into a set of dimensional tables__. 

Data sources:
- directory of JSON logs on user activity on the app
- directory with JSON metadata on the songs in the app

Appropriate fact and dimension tables for a star schema were designed and created in a Postgres database in a former project. The SQL statements are updated for Amazon Redshift.


## Requirements
An AWS account, an AWS IAM user with programmatic access and appropriate rights to use __Infrastructure as Code (IaC)__ and the access key ID and secret access key for the user.

Python 3 with standard packages, boto3, json, configparser and the _psycopg2_ PostgreSQL Python driver. The Postgres driver is the right choice because Redshift is based on Postgres ([source](https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-and-postgres-sql.html)). 

Access to the commandline.


## Instructions
After setting up an AWS account we could create the Amazon Redshift cluster by clicking in the AWS Management Console. When you do this manually you can continue with step 4 after providing information about the new cluster in `dwh.cfg` (at least the redshift_endpoint and arn when the credentials in the section cluster of the configuration file match). 

However, automating the cluster creation has multiple advantages: increased reproducibility and maintainability, allows for multiple deployments with e.g. a test and an identical production environment and sharing is possible.

1. For the __automation of the cluster creation__ an IAM user with programmatic access is needed. In AWS IAM we create a user, grant appropriate policies (e.g. _AmazonRedshiftFullAccess, AmazonS3ReadOnlyAccess, IAMFullAccess_ or more restricted rights than in the simple test setting where you might just set _AdministratorAccess_) and we create an access key ID and secret access key for the user.

2. Save the access key ID and secret access key in the AWS section in `dwh.cfg`. 

3. Execute the steps in the Jupyter notebook `Create_Redshift_Cluster.ipynb` which also create an IAM role for Redshift to access S3.

4. Run the Python script to create tables in Redshift. Execute in the command line in the directory where the files are: `python create_tables.py`.

5. Run the ETL script in the command line: `python etl.py`.

Cleaning up the resources: key to not incur unexpected costs is to shut down the Amazon Redshift cluster. You can use the code in the section "Connect to the cluster" of `Create_Redshift_Cluster.ipynb` to shutdown the resources.  

The Amazon Redshift database can now be used for the Sparkify analytics use case.


## Files

`dwh.cfg`:
Configuration file where the credentials from step 1 should be stored. 

`EDA_input_data.ipynb`:
Exploratory Data Analysis (EDA) to understand the input data.

`Create_Redshift_Cluster.ipynb`:
Provides the code to create an AWS Redshift cluster using the __AWS SDK for Python, ie boto3__.

`create_tables.py`: 
Connects to the Redshift cluster and coordinates the dropping and creation of tables using the SQL queries in `sql_queries.py`.

`etl.py`:
Extracts data from the JSON directories with user listening activity (logs) and song metadata, applies transformations and inserts the data into the Redshift database fact and dimension tables. It coordinates this activity using the SQL queries in `sql_queries.py`.

`sql_queries.py`: 
Contains the SQL code to create tables, transform data and to insert records.


## Discussion of approach
The fictitious music streaming startup called Sparkify migrated their database to a cloud data warehouse. They profit from Cloud advantages like no need to invest upfront, rapid provision of resources, scalable computing power and efficient global access. Thus Sparkify can continue to gain analytical insights even so they have grown their user base and song database.


### The DB design
The two directories of JSON data are loaded into two Redshift staging tables with user listening activity `staging_events` and song metadata `staging_songs`. The Redshift COPY commands for the loading step were carefully designed to handle
- the different JSON input (e.g. using the option to provide a JSONPaths file to parse the JSON source data instead of the _auto_ option, [source](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html) and working with the _timeformat_ option)
- performance considerations: we focus here on the ingestion of the data testing different settings while query performance is not the main focus. We set COMPUPDATE and STATUPDATE off ie compression encodings are not changed and table optimising statistics are skipped ([source](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-load.html#copy-statupdate)).

A central fact table `songplays` is linked to four dimension tables: `users`, `songs`, `artists` and `time`. This approach with denormalised data allows for simple queries compared to many joins needed when using e.g. the third normal form.

The startup Sparkify can answer their question what songs users are listening to using the `songplays` fact table. The table contains the `user_id`, `song_id`, `artist_id` to be joined with the non-time dimension tables for further information and a facilitation to work with the `timestamp` in the `time` dimension table.


### Creating the Amazon Redshift database
We use the AWS SDK for Python, ie boto3. The steps are provided in `Create_Redshift_Cluster.ipynb`. 

It reads from `dwh.cfg` (a) the credentials to access the AWS account with boto3 and (b) the Redshift cluster configuration including the hardware requested as well as the name and credentials to be created with the new Redshift cluster. A __new IAM role__ is created to grant Redshift access to data sources, here to S3. Next the Redshift cluster is created and the cluster endpoint and role ARN are written to `dwh.cfg` using `configparser.ConfigParser().set()`. After opening an incoming TCP port the access to the newly created cluster is tested.


### Possible extensions
Possible next steps are to add data quality checks and to create a dashboard for analytic queries on the new database.

A possible extension for the creation of the Redshift cluster is to automate the steps in Jupyter notebook in a Python .py script to be run in the console. Therefore at least a logic has to be implemented which waits to get the new cluster information till the cluster is created.
