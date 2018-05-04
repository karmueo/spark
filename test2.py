# @File  : test2.py
# @Author: 沈昌力
# @Date  : 2018/4/19
# @Desc  :
from pyspark.sql import SparkSession

logFile = "D:/ProgramData/spark-2.3.0-bin-hadoop2.7/README.md"
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()
d = logData.toPandas()
numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()