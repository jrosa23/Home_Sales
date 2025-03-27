# Home Sales Analysis with PySpark

This project demonstrates how to use PySpark to analyze home sales data, including querying, caching, and optimizing performance with partitioned Parquet files.

## Project Overview

The notebook performs the following key operations:
1. Sets up a Spark environment in Google Colab
2. Reads home sales data from an AWS S3 bucket
3. Runs analytical queries on the data
4. Compares performance between cached and uncached queries
5. Demonstrates partitioning data by year built
6. Shows Parquet file usage for optimized storage and querying

## Environment Setup
- Installs Java 11 and Spark 3.5.5
- Configures necessary environment variables
- Initializes a Spark session

## Requirements
Google Colab environment
PySpark 3.5.5
Java 11

## Data Loading
```python
url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
spark.sparkContext.addFile(url)
home_sales_df = spark.read.csv("file://" + SparkFiles.get("home_sales_revised.csv"), header=True, inferSchema=True)
```

## Usage
Run the notebook cells sequentially in Google Colab
Observe query results and runtime comparisons
Experiment with different queries on the cached and parquet datasets

## Performance Optimization
Caching
python
Copy
spark.sql("CACHE TABLE home_sales")
spark.catalog.isCached('home_sales')  # Verify caching
Parquet Partitioning
python
Copy

## Write partitioned data
home_sales_df.write.partitionBy("date_built").parquet("home_sales_partitioned.parquet")

## Read partitioned data
parquet_df = spark.read.parquet("home_sales_partitioned.parquet")

## Performance Comparison
Uncached query runtime: ~0.0001 seconds

Cached query runtime: ~0.00009 seconds

Parquet query runtime: ~0.00008 seconds

# Results
The analysis shows:

Year-by-year pricing trends for different home configurations
The performance benefits of caching (10-20% improvement)
Even better performance with partitioned Parquet files
