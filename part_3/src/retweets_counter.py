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
KAFKA_TOPIC = 'Tweets'

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
	{KAFKA_TOPIC: 1},
)

# [
#	(None, '{"tid":1074310692235292672,"screen_name":"Odev110"}'),
#	(None, '{"tid":1074310709134139392,"screen_name":"kirillica1957"}')
#	(None, '{"tid":1074310692235292673,"screen_name":"Odev110"}')
# ] => [
#	'1074310692235292672',
# 	'1074310709134139392',
# 	'1074310692235292673'
# ]
tweets_ids = kafka_stream.map(lambda data: rapidjson.loads(data[1])['tid'])

# [
#	'1074310692235292672',
# 	'1074310709134139392',
# 	'1074310692235292673'
# ] => [
# 	('1074310692235292672', '1074310692235292672'),
#	('1074310709134139392', '1074310709134139392'),
#	('1074310692235292673', '1074310692235292673'),
# ]
tid__retweets_count = tweets_ids.map(lambda tid: (tid, tid))  # Further -> (tid, (tid, retweets count))


def update_retweets_count(new_tids, tid__retweets_count__in_state):
	tid = new_tids[0] if new_tids else tid__retweets_count__in_state[0]
	
	retweet_count = tid__retweets_count__in_state[1] if tid__retweets_count__in_state else 0
	try:
		retweet_count = api.get_status(tid).retweet_count
	except tweepy.error.TweepError:
		# print(f'Tweet #{tid} was deleted')
		pass

	return (tid, retweet_count)

	# TODO(a.telyshev): More interesting variant, but "tweepy.error.RateLimitError":
	# 					https://developer.twitter.com/en/docs/basics/rate-limits.html
	# retweets_count = (
	#	tid__retweets_count__in_state[1]
	#	if tid__retweets_count__in_state else 0
	# )
	# now = datetime.now()
	# retweets = [
	# 	tweet
	# 	for tweet in api.retweets(tid)
	# 	if (now - tweet.created_at).total_seconds() < BATCH_DURATION_SEC  # Skip already processed retweets
	# ]
	# for tweet in retweets:
	# 	print(f'New retweet #{tweet.id}({tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")}) for tweet #{tid}')
	# 
	# return (tid, retweets_count + len(retweets))


tid__retweets_total_count = tid__retweets_count.updateStateByKey(update_retweets_count)

top_tweets_by_retweets = (
	tid__retweets_total_count
		.transform(
			lambda rdd: rdd.sortBy(lambda tid___tid__retweets_count: -tid___tid__retweets_count[1][1])
		)
		.map(lambda tid___tid__retweets_count: tid___tid__retweets_count[1])
	)

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
	for tid__retweets_count in (
		rdd
			.sortBy(lambda tid__retweets_count: -tid__retweets_count[1])
			.take(limit)
	):
		tid = tid__retweets_count[0]
		try:
			tweet = api.get_status(tid)
		except tweepy.error.TweepError:
			print(f'Tweet #{tid} was deleted')
			return

		print(f'\n@{tweet.user.screen_name} tweeted in #{tid}:\n"""\n{tweet.text}\n"""\n')


windowed_data.transform(lambda rdd: rdd.coalesce(1)).foreachRDD(lambda _, rdd: get_and_print_top_tweets_info(rdd))

# Start and deinit later
streaming_context.start()
streaming_context.awaitTermination()

