# @File  : kafka_consumer_manually.py
# @Author: 沈昌力
# @Date  : 2018/4/3
# @Desc  : manually assign the partition list for the consumer
from kafka import TopicPartition
from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.assign([TopicPartition('test', 2)])
msg = next(consumer)