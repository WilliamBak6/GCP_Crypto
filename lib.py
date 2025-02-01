import pip

liste = [
    "pandas",
    "numpy",
    "requests",
    "json5", 
    "python-dotenv",
    "google-cloud",
    "google-cloud-bigquery-connection"
] 

for i in liste :
    pip.main(["install", i])