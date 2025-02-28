import pandas as pd
import sys
import os
import numpy as np
import json
from datetime import datetime, timedelta, date  
import requests
from dotenv import *

from g_connections.environment import environnement
from g_connections.environment import bq_environnement

from google.cloud.bigquery.schema import SchemaField
from google.cloud import bigquery

url = "https://data-api.coindesk.com/spot/v1/latest/tick?market=coinbase&instruments=BTC-USD&apply_mapping=true&groups"

load_dotenv()

api_key = os.getenv("API_KEY2")

headers = {
    'Content-Type': 'application/json',
    'api_key': "{}".format(api_key)
}

response = requests.get(url)
print(response.status_code)
# print(response.content)

dt = response.json()["Data"]
dt = json.dumps(dt)
dt = json.loads(dt)
print(type(dt))

df = pd.DataFrame.from_dict(dt, orient='index')
print(df.head(10))

df.to_csv("coindesk_current.csv", index=True)

bucket_id = "will_crypto"
gs_filepath = "current_tendence/coindesk_current.csv"
local_filepath = "coindesk_current.csv"


environnement.ingestion(bucket_id, gs_filepath, local_filepath)