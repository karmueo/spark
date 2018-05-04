# @File  : traclus_consumer.py
# @Author: 沈昌力
# @Date  : 2018/4/20
# @Desc  :
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from ClusterDetector import ClusterDetector


def start():
    sc = SparkContext("local[4]", "Traclus")
    ssc = StreamingContext(sc, 1)

    ssc.checkpoint("file:///E:/SRC/spark/checkpoint/")


    brokers = "127.0.0.1:9092"
    topics = "test2"
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topics], {"metadata.broker.list": brokers})

    values = directKafkaStream.map(lambda x: x[1])

    obCD = ClusterDetector('E:/SRC/TAD/DataCleaning/SHDATA/0423_Out_Clusters_2.csv', 4)
    def _fun(line):
        params = json.loads(line)
        fClusterType = obCD.DetectCluster_UTM(params['LON'], params['LAT'], 1000)
        return fClusterType

    def _fun2(s):
        return (s, 1)

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    ds = values.map(_fun).map(_fun2).updateStateByKey(updateFunc)

    ds.pprint()
    # values.pprint()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    ssc.stop()


if __name__ == '__main__':
    start()