# Installing required packages
!pip install pyspark
!pip install findspark
!pip install pyarrow==0.14.1
!pip install pandas
!pip install numpy==1.19.5

# PySpark is the Spark API for Python
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

# Check the session
spark

# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')

# Preview a few records
mtcars.head()

# Replace the name of first column
mtcars.rename( columns={'Unnamed: 0':'name'}, inplace=True )

# Load the data into a spark dataframe
sdf = spark.createDataFrame(mtcars) 

# Check the schema
sdf.printSchema()

# Create table view
sdf.createTempView("cars")

# Showing the whole table
spark.sql("SELECT * FROM cars").show()

# Showing a specific column
spark.sql("SELECT mpg FROM cars").show(5)

# Basic filtering query to determine cars that have a high mileage and low cylinder count
spark.sql("SELECT * FROM cars where mpg>20 AND cyl < 6").show(5)

# Aggregating data and grouping by cylinders
spark.sql("SELECT count(*), cyl from cars GROUP BY cyl").show()

## UDF
# Import the Pandas UDF function 
from pyspark.sql.functions import pandas_udf, PandasUDFType

# Define and register UDF
@pandas_udf("float")
def convert_wt(s: pd.Series) -> pd.Series:
    # The formula for converting from imperial to metric tons
    return s * 0.45

spark.udf.register("convert_weight", convert_wt)

# Applying the UDF to the tableview
spark.sql("SELECT *, wt AS weight_imperial, convert_weight(wt) as weight_metric FROM cars").show()

# UDF Example1 (Convert the mpg column to kmpl[Factor : 0.425])
@pandas_udf("float")
def convert_mpg(s: pd.Series) -> pd.Series:
    # The formula for converting from imperial to metric tons
    return s * 0.425

spark.udf.register("convert_mpg", convert_mpg)
spark.sql("SELECT *, wt AS weight_imperial, convert_mpg(mpg) as kmpl FROM cars").show()





