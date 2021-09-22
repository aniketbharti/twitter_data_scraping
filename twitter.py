'''
@author: Souvik Das
Institute: University at Buffalo
'''

import datetime
import tweepy


class Twitter:
    def __init__(self,  consumer_key, consumer_secret, access_token, access_token_secret) -> None:
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(
            self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.today = datetime.date.today()
        self.since = self.today - datetime.timedelta(days=752)

    def _meet_basic_tweet_requirements(self):
        '''
        Add basic tweet requirements logic, like language, country, covid type etc.
        :return: boolean
        '''

        raise NotImplementedError

    def get_tweets_by_poi_screen_name(self, config):
        return tweepy.Cursor(self.api.user_timeline, screen_name=config['screen_name'], since=self.since).items(700)

    def get_tweets_by_lang_and_keyword(self, config):
        '''
        Use search api to fetch keywords and language related tweets, use tweepy Cursor.
        :return: List
        '''
        return tweepy.Cursor(self.api.search, q=config['query'], count=config['count'],
                      since=self.since, until=self.today, tweet_mode='extended', lang=config['lang']).items(500)

    def get_replies(self, config):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        return tweepy.Cursor(self.api.search, q='to:{}'.format(config['username']),
                             since_id=config['tweet_id'], tweet_mode='extended').items()
