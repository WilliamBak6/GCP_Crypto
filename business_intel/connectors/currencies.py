import pandas as pd
import numpy as np 
import os
import requests

import json
import datetime

from g_connections.environment import environnement
from g_connections.environment import bq_environnement

from google.cloud.bigquery.schema import SchemaField
from google.cloud import bigquery

now = datetime.date.today()
before = now - datetime.timedelta(days=820)
print(before)

url = "https://api.coincap.io/v2/assets?limit=150"

response = requests.get(url)
# print(response.json())

first_step = response.json()["data"]

second_step = json.dumps(first_step)
third_step = json.loads(second_step)
print(type(third_step))

df = pd.DataFrame(third_step)
print(df.head())

df.to_csv("raw_coincap_list.csv", index=False)

gs_filepath = "coincap_list/raw_coincap_list.csv"
local_path = "raw_coincap_list.csv"
bucket_name = "will_crypto"

environnement.ingestion(bucket_name=bucket_name, destination_blob_path=gs_filepath, source_file_path=local_path)

project_id = "bi-williams"
dataset_id = "raw_cryptocurrencies_list"
table_id = "raw_coincap_currencies"
bucket_name = "will_crypto"

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.write_disposition = "WRITE_TRUNCATE"

job_config.schema = [
    SchemaField("id", "STRING"),
    SchemaField("rank", "INTEGER"),
    SchemaField("symbol", "STRING"),
    SchemaField("name", "STRING"),
    SchemaField("supply", "STRING"),
    SchemaField("maxSupply", "STRING"),
    SchemaField("marketCapUSD", "STRING"),
    SchemaField("volumeUsd24h", "STRING"),
    SchemaField("priceUsd", "FLOAT64"),
    SchemaField("changePercent24H", "FLOAT64"),
    SchemaField("vwap24H", "FLOAT64"),
    SchemaField("explorer", "STRING")
]

job_config.field_delimiter = ","
job_config.skip_leading_rows = 1

bq_environnement.bq_ingestion(project_id, bucket_name, gs_filepath, table_id, dataset_id, job_config)