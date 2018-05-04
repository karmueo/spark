# @File  : spark_kafka_test.py
# @Author: 沈昌力
# @Date  : 2018/4/3
# @Desc  :

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def start():
    sconf = SparkConf()
    sconf.set('spark.cores.max', 8)
    sc = SparkContext(appName='KafkaDirectWordCount', conf=sconf)
    ssc = StreamingContext(sc, 2)

    brokers = "127.0.0.1:9092"
    topic = 'test'
    kafkaStreams = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": brokers})
    # 统计生成的随机数的分布情况
    result = kafkaStreams.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
    # 打印offset的情况，此处也可以写到Zookeeper中
    # You can use transform() instead of foreachRDD() as your
    # first method call in order to access offsets, then call further Spark methods.
    kafkaStreams.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)
    result.pprint()
    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


offsetRanges = []


def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def printOffsetRanges(rdd):
    for o in offsetRanges:
        print("%s %s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset, o.untilOffset - o.fromOffset))


if __name__ == '__main__':
    start()
