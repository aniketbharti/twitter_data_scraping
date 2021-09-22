'''
@author: Souvik Das
Institute: University at Buffalo
'''

import json
import pandas as pd
from twitter import Twitter
from tweet_preprocessor import TWPreprocessor
from indexer import Indexer
from bson import json_util
import time
import pickle

reply_collection_knob = False
reset_solr = False
save_solr = True


class Scrapper:
    def __init__(self) -> None:
        self.indexer = Indexer()
        if reset_solr:
            self.indexer.do_initial_setup()
            data = Scrapper.read_write_json(
                "read", './configs-files/solr-data-format.json', None)
            if data:
                self.indexer.add_fields(data)
        devconfig_details = Scrapper.read_write_json(
            "read", './configs-files/twitterDev.json', None)
        self.config = Scrapper.read_write_json(
            "read", "./configs-files/config.json", None)
        self.twitter = Twitter(devconfig_details['consumer_key'], devconfig_details['consumer_secret'],
                               devconfig_details['access_token'], devconfig_details['access_token_secret'])

    @staticmethod
    def save_file(data, typename, keyword):
        with open("data/pickle/" + typename+'_'+keyword+".pkl", "wb") as output_file:
             pickle.dump(data, output_file)
     
    @staticmethod
    def read_file(type, keyword):
        with open("data/pickle/" + type+'_'+keyword+".pkl", "rb") as output_file:
             data = pickle.load(output_file)
        return data

    @staticmethod
    def read_write_json(operation, filepath, data):
        if operation == 'read':
            with open(filepath, "r", encoding='utf-8') as json_file:
                data = json.load(json_file)
        else:
            with open(filepath, "w",  encoding='utf-8') as json_file:
                json.dump(data, json_file, default=json_util.default,
                          ensure_ascii=False)
        return data

    def start_method(self, mode):
        if mode == 'pois' or mode == 'both':
            for pois in self.config['pois']:
                screen_name = pois['screen_name']
                print("-------poi started-------->", screen_name)
                status_tracking_object = Scrapper.read_write_json(
                    'read', './configs-files/status-tracker.json', None)
                status_tracking_object["pois"][screen_name] = {}
                poi_tweets = self.twitter.get_tweets_by_poi_screen_name(
                    {'screen_name': screen_name})
                print("----- api finish ------")
                poi_processed_tweets = []
                poi_unprocessed_tweets = []
                number_of_retweet = 0
                list_replies_post = []
                for tweet in poi_tweets:
                    tweet = tweet._json
                    if 'retweet_count' in tweet and tweet['retweet_count'] == 0:
                        number_of_retweet = number_of_retweet+1
                        if 'in_reply_to_status_id' in tweet and tweet['in_reply_to_status_id']:
                            list_replies_post.append(
                                {'tweetid': tweet['id'], 'reply_tweet': tweet['in_reply_to_status_id']})
                        status_tracking_object['pois'][screen_name]['last-tweet_id'] = tweet['id']
                        poi_processed_tweets.append(
                            TWPreprocessor.preprocess(tweet, True, pois['country']))
                        poi_unprocessed_tweets.append(tweet)
                status_tracking_object['pois'][screen_name]['result_count'] = len(
                    poi_unprocessed_tweets)
                status_tracking_object['pois'][screen_name]['number_of_retweet'] = number_of_retweet
                status_tracking_object['pois'][screen_name]['replies_post'] = list_replies_post
                try:
                    if save_solr:
                        self.indexer.create_documents(poi_processed_tweets)
                except:
                    with open("exception.txt", "a+") as file_object:
                        file_object.seek(0)
                        data = file_object.read(100)
                        if len(data) > 0 :
                            file_object.write("\n")
                        file_object.write("poi : "+ screen_name)
                Scrapper.read_write_json(
                    'write', './data/json/pois_'+screen_name+'.json', poi_unprocessed_tweets)
                Scrapper.read_write_json(
                    'write', './data/processed_json/pois_'+screen_name+'.json', poi_processed_tweets)
                Scrapper.read_write_json(
                    'write', './configs-files/status-tracker.json', status_tracking_object)
                Scrapper.save_file(poi_processed_tweets, 'pois', screen_name)
                print("-------poi ended-------->")
                time.sleep(5)
        if mode == 'keywords' or mode == 'both':
            for keyword in self.config['keywords']:
                name = keyword['name']
                status_tracking_object = Scrapper.read_write_json(
                    'read', './configs-files/status-tracker.json', None)
                status_tracking_object["keywords"][name] = {}
                print("-------keyword started-------->", name)
                keyword_tweets = self.twitter.get_tweets_by_lang_and_keyword(
                    {'query': name, 'count': keyword['count'], 'lang': keyword['lang']})
                print("----- api finish ------")
                keyword_processed_tweets = []
                keyword_unprocessed_tweets = []
                number_of_retweet = 0
                list_replies_post = []
                for tweet in keyword_tweets:
                    tweet = tweet._json
                    if 'retweeted' in tweet and tweet['retweeted']:
                        number_of_retweet = number_of_retweet+1
                    if 'in_reply_to_status_id' in tweet and tweet['in_reply_to_status_id']:
                        list_replies_post.append(
                            {'tweetid': tweet['id'], 'reply_tweet': tweet['in_reply_to_status_id']})
                    status_tracking_object['keywords'][name]['last-tweet_id'] = tweet['id']
                    keyword_processed_tweets.append(
                        TWPreprocessor.preprocess(tweet, False))
                    keyword_unprocessed_tweets.append(tweet)
                status_tracking_object['keywords'][name]['result_count'] = len(
                    keyword_unprocessed_tweets)
                status_tracking_object['keywords'][name]['number_of_retweet'] = number_of_retweet
                status_tracking_object['keywords'][name]['replies_post'] = list_replies_post
                try:
                    if save_solr:
                        self.indexer.create_documents(keyword_processed_tweets)
                except:
                    with open("exception.txt", "a+") as file_object:
                        file_object.seek(0)
                        data = file_object.read(100)
                        if len(data) > 0 :
                            file_object.write("\n")
                        file_object.write("keyword : "+ name)    
                Scrapper.read_write_json(
                    'write', './configs-files/status-tracker.json', status_tracking_object)
                Scrapper.read_write_json(
                    'write', './data/json/keywords_'+name+'.json', keyword_unprocessed_tweets)
                Scrapper.read_write_json(
                    'write', './data/processed_json/keywords_'+name+'.json', keyword_processed_tweets)
                Scrapper.save_file(keyword_processed_tweets, 'keywords', name)
                print("-------keyword ended-------->")
                time.sleep(50)


sc = Scrapper()
# keyword = sc.read_file('keywords','कोविशील्ड')
# print(keyword)
# indexer = Indexer()
# indexer.create_documents(keyword)
sc.start_method("keywords")
