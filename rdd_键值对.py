# @File  : rdd_键值对.py
# @Author: 沈昌力
# @Date  : 2018/4/20
# @Desc  :
from pyspark import SparkConf, SparkContext

sc = SparkContext(master="local[8]", appName="rdd_test")


lines = sc.textFile("file:///D:/ProgramData/spark-2.3.0-bin-hadoop2.7/README.md")

#1创建
pairRDD = lines.flatMap(lambda line : line.split(" ")).map(lambda word : (word,1))
pairRDD.cache()
# pairRDD.foreach(print)

#2常用的操作
#2.1 reduceByKey(func)的功能是，使用func函数合并具有相同键的值。
# pairRDD.reduceByKey(lambda a,b : a+b).foreach(print)

#2.2 groupByKey()的功能是，对具有相同键的值进行分组。
# pairRDD.groupByKey().foreach(print)

#2.3 keys()只会把键值对RDD中的key返回形成一个新的RDD。
# pairRDD.keys().foreach(print)

#2.4 values()只会把键值对RDD中的value返回形成一个新的RDD。
# pairRDD.values().foreach(print)

#2.5 sortByKey()的功能是返回一个根据键排序的RDD。
# pairRDD.sortByKey().foreach(print)

#2.6 我们经常会遇到一种情形，我们只想对键值对RDD的value部分进行处理，而不是同时对key和value进行处理。
# 对于这种情形，Spark提供了mapValues(func)，它的功能是，对键值对RDD中的每个value都应用一个函数，但是，key不会发生变化。
# pairRDD.mapValues( lambda x : x+1).foreach(print)

#2.7 join(连接)操作是键值对常用的操作。“连接”(join)这个概念来自于关系数据库领域，因此，join的类型也和关系数据库中的join一样，
# 包括内连接(join)、左外连接(leftOuterJoin)、右外连接(rightOuterJoin)等。最常用的情形是内连接，所以，join就表示内连接。
# 对于内连接，对于给定的两个输入数据集(K,V1)和(K,V2)，只有在两个数据集中都存在的key才会被输出，最终得到一个(K,(V1,V2))类型的数据集。
pairRDD1 = sc.parallelize([('spark',1),('spark',2),('hadoop',3),('hadoop',5)])
pairRDD2 = sc.parallelize([('spark','fast')])
pairRDD1.join(pairRDD2).foreach(print)