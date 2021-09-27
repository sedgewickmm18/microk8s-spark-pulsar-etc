from confluent_kafka import Producer
import argparse
import uuid
import json
import time
import random
import pandas as pd


def generate_random_time_series_data(count, act_temp, act_time):
    current_temp = act_temp + random.gauss(0,1)
    current_time = act_time + pd.Timedelta("5 seconds")
    new_message = {"temperature":current_temp, "maxval": 0., "timestamp": str(current_time), "assetId":random.randint(15,15)}
    return count+1, current_temp, current_time, new_message


def kafka_producer(totnr):
    bootstrap_servers = "10.152.183.177:9092"
    topic = "hit_count"
    p = Producer({'bootstrap.servers': bootstrap_servers})
    act_temp = 24.0
    act_time = pd.Timestamp.now()
    total = totnr
    count =0
    while total:
        count, act_temp, act_time, base_message = generate_random_time_series_data(count, act_temp, act_time)
        total-=1

        record_key = str(uuid.uuid4())
        record_value = json.dumps(base_message)

        p.produce(topic, key=record_key, value=record_value)
        p.poll(0)

    p.flush()
    print('we\'ve sent {count} messages to {brokers}'.format(count=count, brokers=bootstrap_servers))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get total nr of messages.')
    parser.add_argument('total', metavar='N', type=int, default=1, nargs='+', help='number of messages')
    args = parser.parse_args()
    kafka_producer(args.total[0])
