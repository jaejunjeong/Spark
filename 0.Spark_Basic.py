# Installing required packages
!pip install pyspark
!pip install findspark

#import findspark and run
import findspark
findspark.init()

# PySpark is the Spark API for Python. We use PySpark to initialize the spark context. 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

## Create the Spark Context and initialize the Spark session needed for SparkSQL and DataFrames
# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#Verify that the spark session instance has been created
spark

##RDDs are Spark's primitive data abstraction and we use concepts from functional programming to create and manipulate RDDs.
#create an RDD here by calling sc.parallelize()
data = range(1,30)

# print first element of iterator
print(data[0])
print(len(data))
xrangeRDD = sc.parallelize(data, 4)

# this will let us know that we created an RDD
xrangeRDD
