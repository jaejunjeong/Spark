--download the csv
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/cars.csv

--kill previously runnin containers
for i in `docker ps | awk '{print $1}' | grep -v CONTAINER`; do docker kill $i; done

--remove any previously used containers
docker rm spark-master spark-worker-1 spark-worker-2

--start the Spark master server
docker run 
    --name spark-master 
    -h spark-master 
    -e ENABLE_INIT_DAEMON=false 
    -p 4040:4040 
    -p 8080:8080 
    -v `pwd`:/home/root 
    -d bde2020/spark-master:3.1.1-hadoop3.2
    
 --start the Spark worker connected to master   
docker run 
    --name spark-worker-1 
    --link spark-master:spark-master 
    -e ENABLE_INIT_DAEMON=false 
    -p 8081:8081 
    -v `pwd`:/home/root 
    -d bde2020/spark-worker:3.1.1-hadoop3.2

--launch pySpark shell in the Spark Master container
docker exec 
    -it `docker ps | grep spark-master | awk '{print $1}'` 
    /spark/bin/pyspark 
    --master spark://spark-master:7077
    
--create DF in the shell
df = spark.read.csv("/home/root/cars.csv", header=True, inferSchema=True) \
    .repartition(32) \
    .cache()
df.show()

--check the spark jobs(port number = '4040')

--Define a UDF
from pyspark.sql.functions import udf
import time

@udf("string")
def engine(cylinders):
    time.sleep(0.2)  # Intentionally delay task
    eng = {6: "V6", 8: "V8"}
    return eng[cylinders]

df = df.withColumn("engine", engine("cylinders"))

dfg = df.groupby("cylinders")

dfa = dfg.agg({"mpg": "avg", "engine": "first"})

dfa.show()
    
--revise the error
@udf("string")
def engine(cylinders):
    time.sleep(0.2)  # Intentionally delay task
    eng = {4: "inline-four", 6: "V6", 8: "V8"}
    return eng.get(cylinders, "other")

df = df.withColumn("engine", engine("cylinders"))

dfg = df.groupby("cylinders")

dfa = dfg.agg({"mpg": "avg", "engine": "first"})

dfa.show()

--add a second worker(at the new terminal)
docker run 
    --name spark-worker-2 
    --link spark-master:spark-master 
    -e ENABLE_INIT_DAEMON=false 
    -p 8082:8082 
    -d bde2020/spark-worker:3.1.1-hadoop3.2
    
--rerun the query(at the first terminal)
dfa.show()
