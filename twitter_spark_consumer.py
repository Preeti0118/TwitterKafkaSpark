import findspark
#change the path as per the installtion on your local machine
findspark.init('/Users/psehgal/dev/spark/env/spark-2.4.5-bin-hadoop2.7')

import os
# you will get java port error without below command
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#for detailed documentation please refer the following url -
#https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html
n_secs = 5
topic = "streamingtweets"

conf = SparkConf().setAppName("teslatweets").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, n_secs)

kafkaStream = KafkaUtils.createDirectStream(ssc, [topic],
                                      {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mystreaminggroup1', #Group id is optional
        # 'fetch.message.max.bytes': '15728640', #optional message size
        # 'auto.offset.reset': 'largest' # optional
        }
        )

lines = kafkaStream.map(lambda x: x[1])

counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.pprint()

ssc.start()

time.sleep(120)  # Terminate after 2 minutes if producer is not detected
# ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)