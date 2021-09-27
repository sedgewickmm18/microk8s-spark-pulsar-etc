import logging
import faust
import json
from typing import Set
from faust.types import AppT, TP

import numpy as np
import scipy as sp
from scipy import stats
import pandas as pd

from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import base
from iotfunctions import bif
from iotfunctions import entity
from iotfunctions import metadata
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.dbtables import FileModelStore
from iotfunctions.enginelog import EngineLogging
from mmfunctions import anomaly

app = faust.App('hit_counter',broker="kafka://10.152.183.177:9092")


class hitCount(faust.Record,validation=True):
    temperature: float
    maxval: float
    timestamp: str
    assetId: str


class DatabaseDummy:
    tenant_id = '###_IBM_###'
    db_type = 'db2'
    model_store = FileModelStore()
    def _init(self):
        return

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

        #<hitCount: temperature=25.3, maxval=0.0, timestamp='1632738034.6980035', assetId='15'>

        # get old value from table
        descriptor = None
        try:
            desc = hits_table[str(hit.assetId)]
            print(desc)
            descriptor = json.loads(desc)
        except Exception as e:
            print(e)
            descriptor = {}
            descriptor[hit.assetId] = []

        # append new value
        descriptor[hit.assetId].append({'temperature': hit.temperature, 'timestamp': hit.timestamp})

        if len(descriptor[hit.assetId]) > 60:

            # turn window into proper input for anomaly scoring
            df = pd.DataFrame.from_records(descriptor[hit.assetId])
            df['entity'] = hit.assetId
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index(['entity','timestamp'], inplace=True)

            print('Dataframe columns', df.columns)
            print('describe ', df.describe())

            # prep
            db_schema=None
            db = DatabaseDummy()
            print (db.tenant_id)

            # call Spectral
            jobsettings = { 'db': db, '_db_schema': 'public', 'save_trace_to_file' : True}

            EngineLogging.configure_console_logging(logging.DEBUG)

            #spsi = anomaly.SpectralAnomalyScoreExt('temperature', 12, ['spectral', 'spectralinv'])
            spsi = anomaly.AnomalyScorer('temperature', 12, ['spectral'])
            et = spsi._build_entity_type(columns = [Column('temperature',Float())], **jobsettings)
            spsi._entity_type = et
            df = spsi.execute(df=df)
            EngineLogging.configure_console_logging(logging.WARNING)

            print(df.columns)

            # return something back - could be a json encoded df
            a = np.amax(np.abs(df['spectral']))
            hit.maxval = max(hit.maxval, a)

            print('Send ', hit.maxval)
            await count_topic.send(value=hit)
            descriptor[hit.assetId] = []

        hits_table[str(hit.assetId)] = json.dumps(descriptor)



@app.agent(count_topic)
async def increment_count(counts):
    print ('Received ', counts)
    async for count in counts:
        print(f"Count in internal topic is {count}")
        count_table[str(count.assetId)]+=1
        print(f'{str(count.assetId)} has now been seen {count_table[str(count.assetId)]} times')

