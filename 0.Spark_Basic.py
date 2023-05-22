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

# subRDD reduce each element in the RDD by 1. filterdRDD then filter the RDD to only contain elements <10.
subRDD = xrangeRDD.map(lambda x: x-1)
filteredRDD = subRDD.filter(lambda x : x<10)

'''
Transformation functions
map(func) - apply function to element (eg. rdd.map(x+2) : apply + 2 to all elements)
flatmap(func) - simillar to map, but can map each input item to zero or more input items. Func should retrun Seq rather than a single item.
filter() - filter the elements
distinct() - filter the distinct elements 
union() - union two RDD 
intersection() - intersection two RDD
cache() - cache the RDD for recalculation
'''

'''
Actions functions
collect() - return all data in RDD 
count() - return count of data in RDD
top(Num) - return top Num of data in RDD
takeOrdered(num)(Ordering) - return ordered Num of data in RDD
reduce(func) - The values of the RDD are calculated in parallel. (Based on calculation: func)
'''

# Transformation returns a result to the driver. Apply the collect() action to get the output from the transformation.
print(filteredRDD.collect())
filteredRDD.count()

##How to create an RDD and cache it. 10x speed improvement. Browse to the Spark UI(host:4040) to see the actual computation time. Second calculation took much less time.
import time 
test = sc.parallelize(range(1,50000),4)
test.cache()

t1 = time.time()
# first count will trigger evaluation of count *and* cache
count1 = test.count()
dt1 = time.time() - t1
print("dt1: ", dt1)


t2 = time.time()
# second count operates on cached data only
count2 = test.count()
dt2 = time.time() - t2
print("dt2: ", dt2)

##Work with the extremely powerful SQL engine in Apache Spark
# Download the data first into a local `people.json` file
!curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/people.json >> people.json

# Read the dataset into a spark dataframe using the `read.json()` function
df = spark.read.json("people.json").cache()

# Print the dataframe as well as the data schema
df.show()
df.printSchema()

# Register the DataFrame as a SQL temporary view
df.createTempView("people")

# Select and show basic data columns(DF and Spark)
df.select("name").show()
df.select(df["name"]).show()
spark.sql("SELECT name FROM people").show()

# Perform basic filtering(DF and Spark)
df.filter(df["age"] > 21).show()
spark.sql("SELECT age, name FROM people WHERE age > 21").show()

# Perfom basic aggregation of data
df.groupBy("age").count().show()
spark.sql("SELECT age, COUNT(age) as count FROM people GROUP BY age").show()

# Stop the Spark session
spark.stop() 



