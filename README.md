import os

# Define Spark version
spark_version = 'spark-3.5.3'
os.environ['SPARK_VERSION'] = spark_version
Purpose:

Sets up the correct Spark version to be used for running PySpark.
1. Install Dependencies & Set Up Environment
python
Copy
Edit
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz
!tar xf $SPARK_VERSION-bin-hadoop3.tgz
!pip install -q findspark
 Purpose:

2. Installs Java 11 (required for Spark).
Downloads and extracts Apache Spark binaries.
Installs findspark, which helps Python locate Spark.
 Set Up Environment Variables
python
Copy
Edit
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop3"
 Purpose:

3. Sets up Java Home and Spark Home environment variables so that Spark can run correctly.
Initialize Spark & Import Libraries
python
Copy
Edit
import findspark
findspark.init()

from pyspark.sql import SparkSession
import time
Purpose:

4. Initializes Spark using findspark.init().
Imports SparkSession (used to create a connection to Spark).
Imports time (used to measure execution time of queries).
5. Create a Spark Session
python
Copy
Edit
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
 Purpose:

Creates a SparkSession, the entry point for using Spark SQL.
The appName("SparkSQL") sets the application name.
6️. Load Data from AWS S3
python
Copy
Edit
from pyspark import SparkFiles

url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"
spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get("home_sales_revised.csv"), header=True, inferSchema=True)
Purpose:

Fetches a CSV file from an AWS S3 bucket.
Uses inferSchema=True to automatically detect column data types.
7️. Create a Temporary SQL View
python
Copy
Edit
df.createOrReplaceTempView("home_sales")
Purpose:

Converts the DataFrame into a temporary SQL table (home_sales) for querying with Spark SQL.
8️.Perform SQL Queries & Analysis
a) Average Price of a 4-Bedroom House Per Year
python
Copy
Edit
query_1 = """
SELECT year(date_built) AS year_built, ROUND(AVG(price), 2) AS avg_price
FROM home_sales
WHERE bedrooms = 4
GROUP BY year_built
ORDER BY year_built
"""
spark.sql(query_1).show()
 Purpose:

Extracts average home price for 4-bedroom homes per year.
b) Average Price of 3-Bedroom, 3-Bathroom Homes Per Year
python
Copy
Edit
query_2 = """
SELECT year(date_built) AS year_built, ROUND(AVG(price), 2) AS avg_price
FROM home_sales
WHERE bedrooms = 3 AND bathrooms = 3
GROUP BY year_built
ORDER BY year_built
"""
spark.sql(query_2).show()
Purpose:

Filters homes with 3 bedrooms & 3 bathrooms.
c) Effect of Square Footage & Floors on Home Price
python
Copy
Edit
query_3 = """
SELECT year(date_built) AS year_built, ROUND(AVG(price), 2) AS avg_price
FROM home_sales
WHERE bedrooms = 3 AND bathrooms = 3 AND floors = 2 AND sqft_living >= 2000
GROUP BY year_built
ORDER BY year_built
"""
spark.sql(query_3).show()
 Purpose:

Adds additional filters for number of floors and minimum square footage.
9️. Measure Query Performance
python
Copy
Edit
start_time = time.time()

# Run SQL Query
spark.sql(query_3).show()

print("--- %s seconds ---" % (time.time() - start_time))
 Purpose:

Measures how long the SQL query takes to execute.
10. Optimize Performance with Caching
python
Copy
Edit
spark.sql("CACHE TABLE home_sales")
spark.catalog.isCached("home_sales")
Purpose:

Caches the table in memory to speed up repeated queries.
isCached("home_sales") checks if caching was successful.
1️11. Partition Data with Parquet Format
python
Copy
Edit
df.write.mode("overwrite").partitionBy("date_built").parquet("home_sales_partitioned")
df_partitioned = spark.read.parquet("home_sales_partitioned")
df_partitioned.createOrReplaceTempView("home_sales_parquet")
 Purpose:

Stores data in Parquet format (optimized for Spark).
Partitions by date_built to speed up queries.
