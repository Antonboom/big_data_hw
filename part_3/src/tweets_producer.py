import logging
import sys

import rapidjson
import tweepy

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

USERS_IDS = ('285532415', '147964447', '34200559', '338960856', '200036850', '72525490', '20510157', '99918629')


class BaseListener(tweepy.StreamListener):

    def __init__(self):
        self.kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS)

    def on_status(self, status):
        logger.warning('Status: %s', status.text)

    def on_error(self, status_code):
        logger.warning(f'Error: %s', status_code)


class TweetsStreamListener(BaseListener):

    def on_data(self, raw_data):
        data = rapidjson.loads(raw_data)

        tid = data.get('id', None)
        if not tid:
            logger.warning('Tweet ID was not found in data: %s', raw_data)
            return True

        user = data.get('user', None)
        if not user:
            logger.warning('User was not found in data: %s', raw_data)
            return True

        screen_name, uid = user['screen_name'], user['id']

        logger.info('Received tweet #%s from @%s[%s]', tid, screen_name, uid)

        self.kafka_producer.send(TWEETS_KAFKA_TOPIC,
            rapidjson.dumps({
                'tid': tid,
                'screen_name': screen_name,
            }).encode('utf-8')
        )
        return True


def main():
    auth = tweepy.OAuthHandler(CONSUMER_TOKEN, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tweepy.API(auth)

    tweets_stream = tweepy.Stream(auth=api.auth, listener=TweetsStreamListener())
    logger.info('Start tweets receiving... Use kafka topic `%s`', TWEETS_KAFKA_TOPIC)
    tweets_stream.filter(follow=USERS_IDS)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.warning('Was stopped by user request')

