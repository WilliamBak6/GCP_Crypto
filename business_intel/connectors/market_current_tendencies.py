import pandas as pd
import os
import sys
import requests

import json  
from dotenv import *
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from dotenv import *

from g_connections.environment import environnement
from g_connections.environment import bq_environnement

url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest"

load_dotenv()
api_key = os.getenv("API_KEY1")

header = {
    'Content-Type': 'application/json',
    'X-CMC_PRO_API_KEY': "{api_key}".format(api_key=api_key)
}

response = requests.get(url, headers=header)
print(response.status_code)
print(response.content)

dt = response.json()["data"]
df = pd.DataFrame(dt)

print(df.head(5))

df.to_csv("current_tendence.csv", index=False)

df = pd.read_csv("current_tendence.csv")

bucket_name = "will_crypto"
local_path = "current_tendence.csv"
gs_filepath = "current_tendence/current_tendency.csv"

environnement.ingestion(bucket_name, gs_filepath, local_path)
