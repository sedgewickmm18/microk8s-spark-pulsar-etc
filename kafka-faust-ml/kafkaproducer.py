from confluent_kafka import Producer
import uuid
import json
import time
import random


def generate_random_time_series_data(count=0):
    new_message = {"hits":random.randint(10,100),"maxval": 0., "timestamp":time.time(),"userId":random.randint(15,15)}
    return count+1,new_message


def kafka_producer():
    bootstrap_servers = "10.152.183.177:9092"
    topic = "hit_count"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    total = 1
    count =0
    while total:
        count,base_message = generate_random_time_series_data(count)
        total-=1

        record_key = str(uuid.uuid4())
        record_value = json.dumps(base_message)

        p.produce(topic, key=record_key, value=record_value)
        p.poll(0)

    p.flush()
    print('we\'ve sent {count} messages to {brokers}'.format(count=count, brokers=bootstrap_servers))

if __name__ == "__main__":
  kafka_producer()
