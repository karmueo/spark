# @File  : kafka_consumer_group.py
# @Author: 沈昌力
# @Date  : 2018/4/3
# @Desc  :join a consumer group for dynamic partition assignment and offset commits
from kafka import KafkaConsumer

consumer = KafkaConsumer('test2', group_id='my_favorite_group')
for msg in consumer:
    print(msg)
