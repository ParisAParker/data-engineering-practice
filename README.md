# Data Engineering Practice

## 1. Project Overview
This project focused on extracting data from an API and storing it in various databases and trying to move that data to different databases

## 2. Workflow

### [X] Step 1: Extract Data from Yahoo Finance API
### Step 2: Store data in different databases
#### Relational Databases:
#### - [X] SQLite3
#### - [X] PostgreSQL

#### NoSQL Databases
#### - [] DynamoDB
#### - [] MongoDB

#### Cloud Based Databases
#### - [] Snowflake
#### - [] Amazon RDS

### Step 3: Create a large CSV (20 million rows) and try to join it to some data stored in the database

#### Notes:
- Pulling data from the API was very easy
- When running a for loop such as:

for row in data.itertuples():
    print(row.Column)

If the column has a space in it, the attribute for that column won't be able to be called off of its name

In order to use SQL in VSCode, I had to download the SQLTools extension which has a dependency of Node.js so I had to download that as well

SQLite can't support multiple schemas like PostgreSQL or Snowflake

In order to connect to PostgreSQL from VSCode need to download "PostgreSQL" extension from Microsoft

Could not do .to_sql() with psycopg2 connection needed to download sqlalchemy
