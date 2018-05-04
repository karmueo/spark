# @File  : kafka_consumer.py
# @Author: 沈昌力
# @Date  : 2018/4/3
# @Desc  : kafka消费者
from kafka import KafkaConsumer

consumer = KafkaConsumer('test2', bootstrap_servers=['127.0.0.1:9092'], auto_offset_reset='latest')
for msg in consumer:
    print(msg)

 # Get consumer metrics
metrics = consumer.metrics()
