#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import string
import threading, logging, time
import sys,traceback
from kafka import KafkaConsumer, KafkaProducer

import io
import avro.schema
import avro.io
import pprint

pp = pprint.PrettyPrinter(indent=4,width=80)


#Variables that contains the user credentials to access Twitter API 
consumer_key = '0SuvevTB3ZMG6u3oqffw'#eWkgf0izE2qtN8Ftk5yrVpaaI
consumer_secret = 'jRBn9XI99X50GoiHyY7e4AHMPV5hrFrYWmcvAQpaHI'#BYYnkSEDx463mGzIxjSifxfXN6V1ggpfJaGBKlhRpUMuQ02lBX

access_token = '1424440200-vZluwSxtJizCP7XdoeyCD8j0dD1oUVPmbAKOGsy'
access_token_secret = 'mw7X3xG7z6LXNo5DSWSErsOmjR3PNLdOCHakHOgHphg'


twitter_schema='''
{"namespace": "example.avro",
 "type": "record",
 "name": "StatusTweet",
 "fields": [
     {"name": "tweet_id"             ,  "type": "long"},
     {"name": "followers_count",  "type": "int"},
     {"name": "statuses_count" ,  "type": "int"},
     {"name": "id_str"         ,  "type": "string"},
     {"name": "friends_count"  ,  "type": "int"},
     {"name": "text"           ,  "type": "string"},
     {"name": "tweet_ts"       ,  "type": "long"},
     {"name": "screen_name"    ,  "type": "string"},
     {"name": "mon", "type": ["null", "int"]} ,
     {"name": "day", "type": ["null", "int"]} ,
     {"name": "hour", "type": ["null", "int"]}
 ]
}
'''

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def __init__(self, topic, broker,numpartitions):

        tweepy.StreamListener.__init__(self)

        self.topic=topic
        self.numpartitions = numpartitions

        # 'ip-172-31-10-235:9092'
        self.broker = broker
        self.id=0
        self.producer =  KafkaProducer(bootstrap_servers=broker)

        self.schema = avro.schema.parse(twitter_schema)
        #self.writer = avro.io.DatumWriter(self.schema)
        #self.bytes_writer = io.BytesIO()
        #self.encoder = avro.io.BinaryEncoder(self.bytes_writer)



    def on_status(self, status):
        
        # Prints the text of the tweet

        writer = avro.io.DatumWriter(self.schema)

        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        
        # Schema changed to add the tweet text
        #print '%d,%d,%d,%s,%s' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.text, status.user.screen_name)
        message =  status.text #+ ',' + status.user.screen_name
        msg = filter(lambda x: x in string.printable, message)
        #print "---"
        #pp.pprint(status)
        #print "---"

        try:

            writer.write(
                {"tweet_id"             : status.id,
                 "followers_count": status.user.followers_count,
                 "friends_count"  : status.user.friends_count  ,
                 "statuses_count" : status.user.statuses_count ,
                 "id_str"         : status.user.id_str	       ,
                 "text"           : msg                        ,
                 "screen_name"    : status.user.screen_name    ,
                 "tweet_ts"       : long(status.timestamp_ms)  ,

                 "mon"            : None,
                 "day"            : None,
                 "hour"           : None                    },encoder)

            raw_bytes = bytes_writer.getvalue()

            self.producer.send(topic=self.topic, value=raw_bytes,key=str(status.id), partition=hash(status.user.screen_name) % self.numpartitions)
            #self.producer.send(topic=self.topic, value=str(msg),key=str(self.id), partition=self.id % self.numpartitions)
#            print 'fc >%d<,frc>%d<,sc>%d<,id>%s<,scn>%s<' % (status.user.followers_count, status.user.friends_count,status.user.statuses_count, status.user.id_str, status.user.screen_name)
            print " topic %s just published %s" % (self.topic, msg)
            self.id += 1
        except Exception, e:
            print " failed %s just published %s" % (self.topic, str(msg))
            print "Unexpected error:", sys.exc_info()[0]
            print '-'*60
            traceback.print_exc(file=sys.stdout)
            print '-'*60
            return True
        
        return True


    def ZZon_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener('twitterstream' , 'ip-172-31-10-235:9092',1 )
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'spark', 'cloudera'])
