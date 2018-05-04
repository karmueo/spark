# @File  : spark_streaming.py
# @Author: 沈昌力
# @Date  : 2018/4/19
# @Desc  :
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Word Count").config("k1", "v1").getOrCreate()
textFile = spark.read.text("file:///D:\\ProgramData\\spark-2.3.0-bin-hadoop2.7\\README.md")

#1
res = textFile.count()
print("textFile.count() = %d" % (res))

#2
res = textFile.first()
print("textFile.first() = %s" % (res))

#3
linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
res = textFile.filter(textFile.value.contains("Spark")).count()
print("textFile.filter(textFile.value.contains(\"Spark\")).count() = %d" % (res))  # How many lines contain "Spark"?

#4
r = split(textFile.value, "\s+")
print(textFile.select(r).show())
e = textFile.select(explode(split(textFile.value, "\s+")).alias("word"))
print(e.show())
g = e.groupBy("word").count()
print(g.show())
print("word counts = %d" %(e.count()))
print("word counts groupby = %d" %(g.count()))

#5
g.cache()
print(g.count())
print(g.count())