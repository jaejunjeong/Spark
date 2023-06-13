--install pyspark
python3 -m pip install pyspark

--get the latest docker-spark
git clone https://github.com/big-data-europe/docker-spark.git

--change directory
cd docker-spark

--start the clsuter
docker-compose up

--create python file 'submit.py'
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

sc = SparkContext.getOrCreate(SparkConf().setMaster('spark://localhost:7077'))
sc.setLogLevel("INFO")

spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(
    [
        (1, "foo"),
        (2, "bar"),
    ],
    StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("txt", StringType(), False),
        ]
    ),
)
print(df.dtypes)
df.show()

--run python3 submit.py at the another terminal
python3 submit.py

--run spark application as port of master(8080)
