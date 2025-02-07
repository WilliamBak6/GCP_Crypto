import pip

liste = [
    "pandas",
    "numpy",
    "requests",
    "json5", 
    "python-dotenv",
    "google-cloud",
    "google-cloud-bigquery-connection",
    "google-cloud-storage",
    "google-api-python-client",
    "google-cloud-bigquery",
    "dbt-core",
    "dbt-bigquery"
] 

for i in liste :
    pip.main(["install", i])