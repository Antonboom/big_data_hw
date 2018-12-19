"""
https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters
follow
For each user specified, the stream will contain:
    * Tweets created by the user.
    * Tweets which are retweeted by the user.
    * Replies to any Tweet created by the user.
    * Retweets of any Tweet created by the user.
    * Manual replies, created without pressing a reply button (e.g. “@twitterapi I agree”)
"""
import logging
import sys

import rapidjson
import tweepy

from tweepy.models import Status
from tweepy.streaming import json
from kafka import KafkaProducer

from local_settings import *

logger = logging.getLogger('tweets_producer')
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s  %(name)s: %(message)s'))
logger.addHandler(handler)


KAFKA_SERVERS = ('localhost:9092',)
TWEETS_KAFKA_TOPIC = 'Tweets'
RETWEETS_KAFKA_TOPIC = 'Retweets'

USERS_IDS = ('285532415', '147964447', '34200559', '338960856', '200036850', '72525490', '20510157', '99918629')


class BaseListener(tweepy.StreamListener):

    def __init__(self, api=None):
        super().__init__(api=api)
        self.kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS)

    def on_status(self, status):
        logger.warning('Status: %s', status.text)

    def on_error(self, status_code):
        logger.warning(f'Error: %s', status_code)


class TweetsStreamListener(BaseListener):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_received_data = None

    def on_data(self, raw_data):
        self._last_received_data = raw_data
        tweet = Status.parse(self.api, json=rapidjson.loads(raw_data))

        if not getattr(tweet, 'user', None):
            logger.warning('User was not found in data: %s', raw_data)
            return True

        if getattr(tweet, 'in_reply_to_status_id', None):
            return self._process_reply(tweet)

        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
        # Retweets can be distinguished from typical Tweets by the existence of a retweeted_status attribute
        is_retweet = getattr(tweet, 'retweeted_status', None)
        if is_retweet:
            if tweet.user.id_str in USERS_IDS:
                return self._process_our_retweet(tweet)
            else:
                return self._process_retweet_on_our_tweet(tweet)

        return self._process_tweet(tweet)

    def _process_tweet(self, tweet):
        self._check_user_is_known(tweet.user)

        logger.info(
            'Received tweet #%s from @%s[%s]',
            tweet.id,
            tweet.user.screen_name,
            tweet.user.id_str,
        )
        self.kafka_producer.send(TWEETS_KAFKA_TOPIC, tweet.user.screen_name.encode('utf-8'))

        return True

    def _process_retweet_on_our_tweet(self, retweet):
        original_tweet = retweet.retweeted_status
        
        if original_tweet.user.id_str not in USERS_IDS:
            logger.warning('Received retweet of retweet of tweet #%s. '
                           'Can not recognize intermediate news channel', retweet.id_str)
            return True

        logger.info(
            "Received retweet #%s of @%s[%s]'s tweet #%s",
            retweet.id_str,
            original_tweet.user.screen_name,
            original_tweet.user.id_str,
            original_tweet.id,
        )
        self.kafka_producer.send(RETWEETS_KAFKA_TOPIC, original_tweet.id_str.encode('utf-8'))

        return True

    def _process_our_retweet(self, retweet):
        self._check_user_is_known(retweet.user)

        original_tweet = retweet.retweeted_status
        logger.info(
            "Received @%s[%s]'s retweet #%s of tweet #%s",
            retweet.user.screen_name,
            retweet.user.id_str,
            retweet.id_str,
            original_tweet.id_str,
        )
        return True

    def _process_reply(self, tweet):
        logger.info(
            "Received reply #%s on @%s[%s]'s tweet #%s",
            tweet.id_str,
            tweet.in_reply_to_screen_name,
            tweet.in_reply_to_user_id_str,
            tweet.in_reply_to_status_id,
        )
        return True

    def _check_user_is_known(self, user):
        if user.id_str not in USERS_IDS:
            logger.warning('User @%s[%s] is unknown. Data: %s', user.screen_name, user.id, self._last_received_data)


def main():
    auth = tweepy.OAuthHandler(CONSUMER_TOKEN, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tweepy.API(auth)

    logger.info('Get users info...')
    user_screen_names = list(sorted([api.get_user(uid).screen_name for uid in USERS_IDS]))
    logger.info('Used tweets kafka topic: `%s`. Used retweets kafka topic: `%s`', TWEETS_KAFKA_TOPIC, RETWEETS_KAFKA_TOPIC)
    logger.info('Start following users: %s', user_screen_names)

    tweets_stream = tweepy.Stream(auth=api.auth, listener=TweetsStreamListener(api=api))
    tweets_stream.filter(follow=USERS_IDS)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.warning('Was stopped by user request')
