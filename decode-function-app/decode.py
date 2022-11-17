
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

import json
import logging
import time
import tempfile
import os
    


import numpy as np
import pandas as pd
from asammdf import MDF
import csv



def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })
    
    
    logging.info('Python EventGrid trigger processed an event: %s', result)
   
    #raw blob details
    raw_STORAGEACCOUNTURL= str(event.get_json()['url']).rsplit('/', 2)[0]
    raw_CONTAINERNAME= event.subject.rsplit('/')[4]
    raw_BLOBNAME= event.subject.rsplit('/')[-1]

    logging.info('raw blob details are A: %s C: %s B: %s', raw_STORAGEACCOUNTURL, raw_CONTAINERNAME, raw_BLOBNAME)

    #create local file to place hold downloaded blog
    #LOCALFILENAME= event.subject.rsplit('/')[-1].replace('.mf4' , '_local.mf4')
    LOCALFILEPATH = '/tmp/'
    LOCALFILE = 'local.mf4'

    try:
        tempFilePath = tempfile.gettempdir()
        LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')


    # Acquire a credential object
    credential = DefaultAzureCredential()

    #download from blob into local file
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=raw_STORAGEACCOUNTURL, credential=credential)
    try:
        blob_client_instance = blob_service_client_instance.get_blob_client(container=raw_CONTAINERNAME, blob=raw_BLOBNAME, snapshot=None)
    except ResourceNotFoundError as e:
       logging.info("No blob found.")

    with open(LOCALFILE.name, "wb") as mf4_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(mf4_blob)
    t2=time.time()
    logging.info(("It takes %s seconds to download "+raw_BLOBNAME) % (t2 - t1))

    # parse mf4 blob
    mf4 = MDF(LOCALFILE.name)
    logging.info('MDF version 4 parsed successfully')

