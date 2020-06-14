This project demostrates the use of KAFKA and SPARK STREAMING technologies for sourcing live tweets using Twitter API and processing the same in Spark.

I have created the following python and Json scripts

1. twitter_streaming_api.py --> This python script acts as the kafka producer and connects with twitter to get live streaming tweets. These tweets are then sent to kafka for queuing which can be eventually read and consumed by kafka consumer

2. twitter_tockens.json --> In order to hide the twitter api secret keys and tokens, I have created a json file twitter_tockens.json where you can save your secret keys. This file is read by the above python script during run time to read the keys and establish the connection with twitter api.

3. twitter_spark_consumer.py --> This python script using spark streaming and acts as a kafka consumer to re=ceive the tweets sent by the kafka producer. It reads the messages from kafka and prints them out by splitting the message in to words with respective counts

4. twitter_streaming_consumer.py --> This is a simple python script that reads data from kafka producer without using spark streaming. This program can be used to identify issues if they exist in kafka producer or spark streaming.


Steps to run the application -
1. Assuming prerequisites for Zookeeper, kafka, and spark streaming installation are complete
2. Start zookeper server
3. Start kafka server
4. Update twitter api keys and tokens in twitter_tockens.json
5. Run twitter_streaming_api.py in CLI
6. Run twitter_spark_consumer.py in CLI
