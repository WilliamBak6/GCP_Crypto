import pip

liste = [
    "pandas",
    "numpy",
    "requests",
    "json5", 
    "python-dotenv",
    "google-cloud"
] 

for i in liste :
    pip.main(["install", i])