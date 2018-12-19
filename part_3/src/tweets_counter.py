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
#	(None, 'RT_russian'),
#	(None, 'rentvchannel')
#	(None, 'RT_russian')
# ] => [
# 	('RT_russian', 2),
#	('rentvchannel', 1)
# ]
user__count = kafka_stream.map(lambda none__user: (none__user[1], 1)).reduceByKey(ssum)


def print_windowed(data_stream, func, time):
	# TODO(a.telyshev): Use countByValueAndWindow?
	windowed_data = data_stream.reduceByKeyAndWindow(func, None, windowDuration=time, slideDuration=time)
	windowed_data.transform(
		lambda rdd: rdd.coalesce(1).sortBy(lambda user__count: user__count[1], ascending=False)
	).pprint(100)

print_windowed(user__count, ssum, MINUTE)
print_windowed(user__count, ssum, MINUTE * 10)

# Start and deinit later
streaming_context.start()
streaming_context.awaitTermination()
