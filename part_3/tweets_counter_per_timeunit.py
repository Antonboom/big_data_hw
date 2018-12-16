import sys

import rapidjson

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

ZOOKEEPER_SERVER = 'localhost:2181'
KAFKA_TOPIC = 'Tweets'

APP_NAME = 'UsersTweetsCounter'
BATCH_DURATION_SEC = 30

MINUTE = 60
TEN_MINUTES = MINUTE * 10

# Init streaming
spark_context = SparkContext(appName=APP_NAME)
spark_context.setLogLevel('ERROR')

streaming_context = StreamingContext(spark_context, BATCH_DURATION_SEC)
streaming_context.checkpoint(f'{APP_NAME}__checkpoint')

# Process tweets
kafka_stream = KafkaUtils.createStream(
	streaming_context,
	ZOOKEEPER_SERVER,
	f'{APP_NAME}__consumers_group',
	{KAFKA_TOPIC: 1},
)

def ssum(a, b):
	return a + b

# [
#	(None, '{"tid":1074310692235292672,"screen_name":"Odev110"}'),
#	(None, '{"tid":1074310709134139392,"screen_name":"kirillica1957"}')
#	(None, '{"tid":1074310692235292673,"screen_name":"Odev110"}')
# ] => [
#	'Odev110',
# 	'kirillica1957',
# 	'Odev110'
# ]
screen_names = kafka_stream.map(lambda data: rapidjson.loads(data[1])['screen_name'])

# [
#	'Odev110',
# 	'kirillica1957',
# 	'Odev110'
# ] => [
# 	('Odev110', 2),
#	('kirillica1957', 1)
# ]
screen_name__count = screen_names.map(lambda name: (name, 1)).reduceByKey(ssum)

def print_windowed(data_stream, func, time):
	windowed_data = data_stream.reduceByKeyAndWindow(func, None, windowDuration=time, slideDuration=time)
	windowed_data.transform(lambda rdd: rdd.coalesce(1).sortByKey(ascending=False)).pprint(100)

print_windowed(screen_name__count, ssum, MINUTE)
print_windowed(screen_name__count, ssum, TEN_MINUTES)

# Start and deinit later
streaming_context.start()
streaming_context.awaitTermination()

