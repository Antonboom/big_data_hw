import sys
import time

from datetime import datetime

import rapidjson
import tweepy

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from local_settings import *

ZOOKEEPER_SERVER = 'localhost:2181'
RETWEETS_KAFKA_TOPIC = 'Retweets'

APP_NAME = 'UsersRetweetsCounter'
MINUTE = 60
BATCH_DURATION_SEC = MINUTE

# Init twitter API
auth = tweepy.OAuthHandler(CONSUMER_TOKEN, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

api = tweepy.API(auth)

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
	{RETWEETS_KAFKA_TOPIC: 1},
)

# [
#	(None, '1074310692235292672'),
#	(None, '1074310709134139392')
#	(None, '1074310692235292672')
# ] => [
#	('1074310692235292672', 2)
# 	('1074310709134139392', 1)
# ]
tid__retweets_count = kafka_stream.map(lambda none__tid: (none__tid[1], 1)).reduceByKey(lambda a, b: a + b)


def update_retweets_count(new_retweets_counts, retweets_count__in_state):
	retweets_count__in_state = retweets_count__in_state or 0
	return sum(new_retweets_counts, retweets_count__in_state)


tid__retweets_total_count = tid__retweets_count.updateStateByKey(update_retweets_count)


def sort_by_retweets_count(tid__retweets_count):
	return tid__retweets_count[1]


top_tweets_by_retweets = tid__retweets_total_count.transform(lambda rdd: rdd.sortBy(sort_by_retweets_count, ascending=False))
# Every minute print 10 of top retweeted tweets and save result in file
top_tweets_by_retweets.pprint(10)
top_tweets_by_retweets.transform(lambda rdd: rdd.coalesce(1)).saveAsTextFiles('file:///tmp/retweets/counts')

# Every 15 minutes print info about 5 of top retweeted tweets
windowed_data = top_tweets_by_retweets.reduceByKeyAndWindow(
	max,
	None,
	windowDuration=MINUTE * 15,
	slideDuration=MINUTE * 15
)


def get_and_print_top_tweets_info(rdd, limit=5):
	for tid__retweets_count in rdd.sortBy(sort_by_retweets_count, ascending=False).take(limit):
		tid = tid__retweets_count[0]
		try:
			tweet = api.get_status(tid)
		except tweepy.error.TweepError:
			print(f'Tweet #{tid} was deleted')
			return

		text = tweet.text
		extended_tweet = getattr(tweet, 'extended_tweet', None)
		if extended_tweet:
			text = extended_tweet.full_text

		print(f'\n@{tweet.user.screen_name} tweeted in #{tid}:\n"""\n{tweet.text}\n"""\n')


windowed_data.transform(lambda rdd: rdd.coalesce(1)).foreachRDD(lambda _, rdd: get_and_print_top_tweets_info(rdd))

# Start and deinit later
streaming_context.start()
streaming_context.awaitTermination()
