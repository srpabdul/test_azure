from azure.storage.blob import BlockBlobService
import pandas as pd
import tables
import time
from tables import exceptions
import yaml

"""
    Yaml is used to store all parameter 
"""
CONFIG_FILE_PATH = "config.yaml"

def load_config():
    with open(CONFIG_FILE_PATH) as f:
        return yaml.safe_load(f.read())["Demo_config"]

data = load_config()

def get_config(source):    
    return data[source]
demo_config = get_config('Demo')
STORAGEACCOUNTNAME=demo_config["STORAGEACCOUNTNAME"]
STORAGEACCOUNTKEY= demo_config["STORAGEACCOUNTKEY"]
LOCALFILENAME= demo_config["LOCALFILENAME"]
CONTAINERNAME= demo_config["CONTAINERNAME"]
BLOBNAME= demo_config["BLOBNAME"]


def fetch_csv():

    t1=time.time()
    try:
            blob_service=BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)
            blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)
            print(blob_service)
            t2=time.time()

            #LOCALFILE is the file path
            dataframe_blobdata = pd.read_csv(LOCALFILENAME)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    fetch_csv()