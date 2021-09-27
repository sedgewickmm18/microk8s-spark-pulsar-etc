import faust
import json
from typing import Set
from faust.types import AppT, TP

import numpy as np
import scipy as sp
from scipy import stats


app = faust.App('hit_counter',broker="kafka://10.152.183.177:9092")


class hitCount(faust.Record,validation=True):
    hits: int
    maxval: float
    timestamp: float
    userId: str


hit_topic = app.topic("hit_count", partitions=1, value_type=hitCount)
count_topic = app.topic('count_topic', internal=True, partitions=1, value_type=hitCount)

hits_table = app.Table('hitCount', partitions=1, key_type=str,value_type=str,default=str)
count_table = app.Table("major-count",key_type=str,value_type=float,partitions=1,default=float)

@app.on_configured.connect
def configure(app, conf, **kwargs):
    #conf.broker = os.environ.get('FAUST_BROKER')
    #conf.store = os.environ.get('STORE_URL')
    print(f'App {app} has been configured.')
    #print(f'Conf {conf}')

#@app.on_after_configured.connect
#def after_configuration(app, **kwargs):
#    print(f'App {app} has been configured.')


@app.agent(hit_topic)
async def count_hits(hits):
    async for hit in hits:
        print(f"Data received is {hit}")

        # get old value from table
        descriptor = None
        try:
            desc = hits_table[str(hit.userId)]
            print(desc)
            descriptor = json.loads(desc)
        except Exception as e:
            print(e)
            descriptor = {}
            descriptor[hit.userId] = []

        # append new value
        descriptor[hit.userId].append(hit.hits)
        print (descriptor[hit.userId])
        if len(descriptor[hit.userId]) > 20:
            a = np.amax(np.abs(sp.stats.zscore(descriptor[hit.userId])))
            hit.maxval = max(hit.maxval, a)
            print('Send ', hit.maxval)
            await count_topic.send(value=hit)
            descriptor[hit.userId] = []
        hits_table[str(hit.userId)] = json.dumps(descriptor)



@app.agent(count_topic)
async def increment_count(counts):
    print ('Received ', counts)
    async for count in counts:
        print(f"Count in internal topic is {count}")
        count_table[str(count.userId)]+=1
        print(f'{str(count.userId)} has now been seen {count_table[str(count.userId)]} times')

