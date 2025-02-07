import os
import json
import pip

# pip.main(["install", "google-cloud-storage"])

from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\HP\Desktop\bi-williams-03ff653c2a2a.json"

cred = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]



def ingestion(bucket_name, destination_blob_path, source_file_path):
    storage_client = storage.Client()

    bucket_name = bucket_name
    destination = destination_blob_path

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(source_file_path)

    return None
