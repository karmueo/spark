# @File  : kafka_producer.py
# @Author: 沈昌力
# @Date  : 2018/4/3
# @Desc  :Kafka生产者
from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers='localhost:9092')

# for _ in range(100):
#     producer.send('test', b'scl first Kafka message')
#     print('scl first Kafka message')

# Block until a single message is sent (or timeout)
# future = producer.send('test', b'scl another_message')
# print('scl another_message')
# result = future.get(timeout=60)
#
# # 阻塞，直到所有待处理的消息放在网络上
# # NOTE: 这不保证提交或成功！ 如果使用linger_ms配置内部批处理，它确实非常有用
# producer.flush()

# # Use a key for hashed-partitioning
# producer.send('test', key=b'foo', value=b'bar')
# print('send key=foo value=bar')

# json消息上传到kafka
import json
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
dict = {'US': 51401351376918, 'BATCH': 7303, 'LON': 121.2171, 'LAT': 31.6480667, 'TIME': '2018/01/01 00:18:35', 'V': 0.05, 'C': -1.0}
producer.send('test2', dict)
print("send json={'US': '111', 'LON':'123', 'LAT':'22'}")
producer.flush(timeout=1000)
print("send done")
 # Serialize string keys
# producer = KafkaProducer(key_serializer=str.encode)
# producer.send('test2', key='ping', value=b'1234')
# print("send string key='ping', value=b'1234'")

# # 压缩消息
# producer = KafkaProducer(compression_type='gzip')
# for i in range(1000):
#     producer.send('test', b'msg %d' % i)
#     print("send gzip msg")

# Get producer performance metrics
# 获取producer性能指标
metrics = producer.metrics()