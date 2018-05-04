# @File  : spark-streaming-quick-example.py
# @Author: 沈昌力
# @Date  : 2018/4/19
# @Desc  :
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[4]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# dict = {'test2':2}
# kafkaStream = KafkaUtils.createStream(ssc, ['127.0.0.1:2181'], ['01'], dict)

brokers = "127.0.0.1:9092"
topics = "test2"
directKafkaStream = KafkaUtils.createDirectStream(ssc, [topics], {"metadata.broker.list": brokers})

values = directKafkaStream.map(lambda x: x[1])
# values.pprint()

def _fun(line):
    params = json.loads(line)
    return params["LON"]

dstream = values.map(_fun)

dstream.pprint()
# counts = lines.map(lambda line: line.split(","))
# counts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
ssc.stop()