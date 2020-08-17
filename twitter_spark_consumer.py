import findspark
#change the path as per the installtion on your local machine
findspark.init('/Users/psehgal/dev/spark/env/spark-2.4.5-bin-hadoop2.7')

import os
# you will get the following java port error without below os.environ command
#Error Message - "Java Gateway process exited before sending the druver its port number"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils




###load Positive and negative word lists

pf = open('./positive.txt', 'r')
p_words = pf.read().split('\n')
pf.close()

nf = open('./negative.txt', 'r')
n_words = nf.read().split('\n')
nf.close()

###Set up Spark Direct Receiver
#for detailed documentation please refer the following url -
#https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html
#First, we import StreamingContext, which is the main entry point for all streaming functionality.
n_secs = 5
topic = "streamingtweets"

conf = SparkConf().setAppName("teslatweets").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, n_secs)


#Create Kafka streaming object
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic],
                                      {
                                        'bootstrap.servers': 'localhost:9092'
                                        }
                                        )

kafkaStream.pprint()
lines = kafkaStream.map(lambda x: x[1])#encode('ascii','ignore'))

counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.pprint()

# pos_sentiments = lines.flatMap(lambda line: line.split(" ")).map(lambda word: ('Positive', 1) if word in p_words else ('Positive', 0)).reduceByKey(lambda a, b: a + b)
# neg_sentiments = lines.flatMap(lambda line: line.split(" ")).map(lambda word: ('Negative', 1) if word in n_words else ('Negative', 0)).reduceByKey(lambda a, b: a + b)
words = lines.flatMap(lambda line: line.split(" "))
pos_sentiments = words.map(lambda word: ('Positive', 1) if word in p_words else ('Positive', 0)).reduceByKey(lambda a, b: a + b)
neg_sentiments = words.map(lambda word: ('Negative', 1) if word in n_words else ('Negative', 0)).reduceByKey(lambda a, b: a + b)
pos_sentiments.pprint()
neg_sentiments.pprint()

# all_sentiment_counts = pos_sentiments.union(neg_sentiments).reduceByKey(lambda a, b: a + b)
# all_sentiment_counts.pprint()

# historical_counts = all_sentiment_counts.updateStateByKey(updateFunction)

# counts = []
# all_sentiment_counts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

ssc.start()

# time.sleep(300) # Terminate after 5 minutes if producer is not detected

ssc.awaitTermination() #this can be used if the system is required to wait until terminated

ssc.stop(stopSparkContext=True, stopGraceFully=True)