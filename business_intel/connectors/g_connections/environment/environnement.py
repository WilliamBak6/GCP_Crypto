import os
import json
import pip

# pip.main(["install", "google-cloud-storage"])

from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\HP\Desktop\bi-williams-03ff653c2a2a.json"

cred = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

storage_client = storage.Client()
