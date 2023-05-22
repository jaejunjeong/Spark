# Installing required packages
!pip install pyspark
!pip install findspark
!pip install pandas

import findspark
findspark.init()

import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Check spark instance is created
spark

# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')

# Preview a few records
mtcars.head()

# We use the `createDataFrame` function to load the data into a spark dataframe
sdf = spark.createDataFrame(mtcars) 

# Let us look at the schema of the loaded spark dataframe
sdf.printSchema()

# Preview the first 5 records
sdf.show(5)

# Select() function to select a particular column of data
sdf.select('mpg').show(5)

# Use the filter() function to filter the data
sdf.filter(sdf['mpg'] < 18).show(5)

# Create a new column called wtTon that has the weight from the wt column converted to metric tons
sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)

# Compute the average weight of cars by their cylinders
sdf.groupby(['cyl']).agg({"wt": "AVG"}).show(5)

# Sort the output from the aggregation to get the most common cars
car_counts = sdf.groupby(['cyl']).agg({"wt": "count"}).sort("count(wt)", ascending=False).show(5)

# Example1("Print out the mean weight of a car in our database in metric tons")
sdf.withColumn('wtTon', sdf['wt'] * 0.45).groupby(["Unnamed: 0"]).agg({"wtTon": "AVG"}).show()

# Example2("create a new column for mileage in kmpl instead of mpg(miles-per-gallon) by using a conversion factor of 0.425, and sort descending by kmpl 
sdf.withColumn('kmpl', sdf['mpg'] * 0.425).sort("kmpl", ascending=False).show()
