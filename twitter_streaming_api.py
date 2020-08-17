from kafka import SimpleProducer, SimpleClient, KafkaProducer
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from json import dumps

############################
##  Create listener class
############################

class listener(StreamListener):
    """
    This class is created by inheriting StreamListnerclass from tweepy
    This class works as a kafka producer and reads streaming tweets based on the set filter and sends the same to
    kafka messaging server
    """

    def __init__(self):

        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            # value_serializer=lambda m: dumps(m).encode('ascii'))
                            value_serializer = lambda m: dumps(m).encode('utf-8'))

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        # msg = data.text.encode('utf-8')
        #print(data)

        start = data.find('"text":') + len('"text":')
        end = data.find('"source"')
        substring = data[start:end]
        print(data)
      #  print(substring)

        try:
            self.producer.send('streamingtweets', substring)  # topic=streamingtweets, msg=substring
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status):
        print("Error ", status)
        return True  # This will avoid stream from getting interrupted

    def on_timeout(self):
        return True  # This will avoid stream from getting interrupted


if __name__ == '__main__':
    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    #                          value_serializer=lambda m: dumps(m).encode('ascii'))

    #####################################
    ##  Load API Tokens from json file
    #####################################

    # First step is to read the secret keys and tokens for authenticating with tweeter
    #The tiockens and secret keys are stored in a separte json file for security purposes
    tokenfile = open('./twitter_tokens.json')
    tokens = json.load(tokenfile)
    ckey = tokens['ckey']
    csecret = tokens['csecret']
    atoken = tokens['atoken']
    asecret = tokens['asecret']

    # Using the secret keys/tokens create an auth object
    authobject = OAuthHandler(ckey, csecret)
    authobject.set_access_token(atoken, asecret)

    # Using the listener class create a Stream object
    twitterstream = Stream(authobject, listener())

    #connect with Twitter API using the stream object and filter the tweets for Tesla
    twitterstream.filter(track=["$TSLA"])



    # twitterstream.filter(track=["$TSLA", "$F"], languages = ["en") #You can have more than one filter
    # twitterstream.filter(locations=[-180, -90, 180, 90], languages=['en']) #You can also filter by location
