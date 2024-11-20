# ETL Pipeline for Airbnb Data using AWS Glue and Spark

## Project Overview

This project implements an **ETL (Extract, Transform, Load)** pipeline for processing Airbnb listing data using **AWS Glue** and **Apache Spark**. The project extracts raw data from an S3 bucket, performs necessary transformations, and loads the cleaned data back into S3 for further analysis. This pipeline is fully managed in AWS, leveraging services like **AWS S3**, **AWS Glue**, and **IAM** for efficient data processing.

## Steps to Implement the ETL Pipeline

### Step 1: Create IAM Role for the Project

- **IAM (Identity and Access Management)** roles define the permissions required for AWS services to access resources.
- We created a role with **AdministratorAccess** to allow AWS Glue to perform operations like reading from and writing to S3.

  <img width="957" alt="image" src="https://github.com/user-attachments/assets/273d2ab0-4cf7-49f2-ae9e-bca6b33a7a76">



### Step 2: Create an S3 Bucket for Data Storage

- **Amazon S3** is used for storing both raw and transformed data. The following structure is created:
  - S3 bucket: `new-york-city-airbnb-2023`
  - Folder for raw data: `NYC-airbnb-2023/row-data`
  - Folder for transformed data: `NYC-airbnb-2023/transformed-data`

### Step 3: Set up AWS Glue Database and Table

- **AWS Glue** is used to catalog the data and run ETL jobs. The data from S3 is crawled to create a schema.
- **Glue Database**: `etl-pipeline-project`
- **Crawler**: `elt-airbnb-data-pipeline`

### Step 4: Create ETL Job in AWS Glue

- The ETL job is created in AWS Glue using Apache **Spark** as the execution engine.
- The job extracts data from the raw S3 folder, applies transformations, and writes the results to the transformed data folder.

### Step 5: Write PySpark Code for Data Transformation

- **PySpark** is used to load the raw data from S3, transform it (cleaning, type casting, aggregation), and then save the transformed data back to S3.
- Example transformations:
  - Converting column data types (e.g., price, reviews, etc.)
  - Dropping columns with excessive null values
  - Aggregating room types and calculating total earnings per room type.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName('AirbnbETL').getOrCreate()

# Read raw data from S3
df = spark.read.csv('s3://new-york-city-airbnb-2023/NYC-airbnb-2023/row-data/', header=True, inferSchema=True)

# Data transformation: Type casting
df_transformed = df.withColumn('price', df['price'].cast(IntegerType()))

# Save transformed data to S3
df_transformed.write.csv('s3://new-york-city-airbnb-2023/NYC-airbnb-2023/transformed-data/')

```

## Final Outputs:
- **Cleaned Data**: The transformed data is saved in CSV format in the `transformed-data` folder of the S3 bucket. The folder contains the following CSV files:
  - **airbnb-dataframe.csv**: Contains the transformed Airbnb listing data with correct data types.
  - **count-room-type.csv**: Contains the counts of different room types available in the dataset.
  - **total-earn-room-type.csv**: Contains the total earnings calculated for each room type.
  - **droping.csv**: Contains the dataset with columns dropped due to a high number of null values.

## Conclusions:
- **Data Transformation**: The transformed dataset provides clean, consistent data that can be easily used for analysis and machine learning tasks. The transformation process ensured that columns had appropriate data types and unnecessary columns were removed.
- **Room Type Distribution**: The `count-room-type.csv` file offers valuable insights into the distribution of room types in the dataset, which can help businesses understand the variety of listings.
- **Earnings by Room Type**: The `total-earn-room-type.csv` file helps in analyzing the earnings for different room types, which could inform pricing and business strategies for Airbnb hosts.
- **Data Quality Improvement**: The `droping.csv` highlights how cleaning the data by removing columns with excessive missing values can enhance data quality, making it more suitable for downstream analysis.
