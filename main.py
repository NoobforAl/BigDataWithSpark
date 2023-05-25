from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType
from pyspark.sql.functions import udf, explode, count
from pyspark.sql import SparkSession

from ipaddress import ip_address
from IP2Location import IP2Location

import sys
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

schema = StructType([
    StructField("start", IntegerType(), False),
    StructField("end", IntegerType(), False)])

#setup your server master!!
spark = SparkSession\
    .builder\
    .master("spark://192.168.99.1:7077")\
    .appName("asdfewcva")\
    .config("spark.cores.max", 4)\
    .config("spark.driver.maxResultSize", "512m")\
    .config("spark.executor.memory", "512m") \
    .config("spark.executor.cores", 1) \
    .getOrCreate()


@udf(returnType=ArrayType(StringType()))
def findIPs(start, end):
    db = "your-path/DB.BIN"
    fIP = IP2Location(db)

    if None in (start, end):
        return ["-"]

    S = ip_address(start)
    E = ip_address(end)
    result = []
    for i in range(0, 60):
        if S >= E:
            break
        if "-" != fIP.find(str(S+i)).country_short:
            result.append(d)
    return result


csvFile = "your-path/Data-Eng/end_ips.csv"
df = spark.read \
    .schema(schema=schema)\
    .format("csv")\
    .option("header", True)\
    .load(csvFile)


con = df\
    .withColumn("listIp",
                findIPs(df.start, df.end))\
    .drop("start", "end")\
    .select("listIp",
            explode("listIp")) \
    .drop("listIp")\
    .dropna()\
    .cache()

con = con.groupBy('col')\
    .agg(count('col')
         .alias('count')) \
    .sort('count')\
    .cache()

con.toPandas().to_csv('./out/out.csv')

