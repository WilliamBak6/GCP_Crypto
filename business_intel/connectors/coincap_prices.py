import pandas as pd 
import requests
from datetime import *
import json
import os
import sys

from g_connections.environment import environnement
from g_connections.environment import bq_environnement

from google.cloud.bigquery.schema import SchemaField
from google.cloud import bigquery

now = datetime.now()
first_date = now - timedelta(days=380)

ts_now = int(datetime.timestamp(now) * 1000)
ts_start = int(datetime.timestamp(first_date) * 1000)

data = pd.read_csv("raw_coincap_list.csv")
data = data.dropna(subset="changePercent24Hr")
print(data.head(10))
liste = data["id"]
liste = list(liste)
print(liste[0])

df = pd.DataFrame()
j = -1

for i in liste: 
    url = "https://api.coincap.io/v2/assets/{coin}/history?interval=d1&start={start}&end={end}".format(coin=i, start=ts_start, end=ts_now)

    response = requests.get(url)
    # print(response.status_code)

    data0 = response.json()["data"]
    df1 = pd.DataFrame(data0)
    df1["id"] = i
    df = pd.concat([df, df1])

df.to_csv("coincap_historical_prices.csv", index=False)

bucket_name = "will_crypto"
local_path = "coincap_historical_prices.csv"
gs_filepath = "coincap_hist/raw_coincap_prices.csv"
project_id = "bi-williams"
table_id = "raw_coincap_prices"
bucket_name = "will_crypto"
dataset_id = "raw_cryptocurrencies_list"

environnement.ingestion(bucket_name=bucket_name, destination_blob_path=gs_filepath, source_file_path=local_path)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.write_disposition = "WRITE_TRUNCATE"
job_config.schema = [
    SchemaField("priceUSD", "STRING"),
    SchemaField("time", "INTEGER"),
    SchemaField("date", "STRING"),
    SchemaField("id", "STRING")
]
job_config.field_delimiter = ","
job_config.skip_leading_rows = 1

bq_environnement.bq_ingestion(project_id=project_id, bucket_name=bucket_name, uri=gs_filepath, table_id=table_id, dataset_id=dataset_id, job_config=job_config)
