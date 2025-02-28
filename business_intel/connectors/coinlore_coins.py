import requests
import pandas as pd 
import os
from dotenv import *

import json 
from g_connections.environment import bq_environnement
from g_connections.environment import environnement

from google.cloud.bigquery.schema import SchemaField
from google.cloud import bigquery

df = pd.DataFrame()

for i in [0, 100]:
    j = i + 100
    url = 'https://api.coinlore.net/api/tickers/?start={i}&limit={j}'.format(i=i,j=j)
    response = requests.get(url)
    print(response.status_code)
    data = response.json()["data"]
    df1 = pd.DataFrame(data)
    df = pd.concat([df, df1])

df.to_csv("coinlore_coin.csv", index=False)

df = pd.read_csv("coinlore_coin.csv")

bucket_name = "will_crypto"
gs_filepath = "coinlore_list/coinlore_list.csv"
local_path = "coinlore_coin.csv"

environnement.ingestion(bucket_name, gs_filepath, local_path)

project_id = "bi-williams"
table_id = "raw_coinlore_coins"
bucket_name = "will_crypto"
dataset_id = "raw_cryptocurrencies_list"

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = "WRITE_TRUNCATE"
job_config.field_delimiter = ","
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.schema = [
    SchemaField("id", "INTEGER"),
    SchemaField("symbol", "STRING"),
    SchemaField("name", "STRING"),
    SchemaField("nameid", "STRING"),
    SchemaField("rank", "INTEGER"),
    SchemaField("priceUSD", "FLOAT64"),
    SchemaField("percent_change_24h", "FLOAT64"),
    SchemaField("percent_change_1h", "FLOAT64"),
    SchemaField("percent_change_7d", "FLOAT64"),
    SchemaField("price_btc", "STRING"),
    SchemaField("market_cap_usd", "STRING"),
    SchemaField("volume24h", "STRING"),
    SchemaField("volume24a", "FLOAT64"),
    SchemaField("csupply", "STRING"),
    SchemaField("tsupply", "STRING"),
    SchemaField("msupply", "STRING")
    
]

bq_environnement.bq_ingestion(project_id, bucket_name, gs_filepath, table_id, dataset_id, job_config)
    