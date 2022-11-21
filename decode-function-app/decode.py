import json
import csv
import time
import logging
import tempfile
import gzip
import numpy as np
import pandas as pd
from asammdf import MDF
from datetime import datetime, timedelta

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


# Writes a gzipped CSV file using the uuid as name
def writeCsv(csv_file, mdf, target_file):
    # open the file in the write mode
    with gzip.open(csv_file, 'wt') as csvFile:

        logging.info("Exporting to: " + csvFile.name + '-signalscsv.gz' )

        writer = csv.writer(csvFile)
        writer.writerow(["source_file","source_uuid", "name", "unit", "relativeTimestamp", "absoluteTimestamp", "value", "value_string", "source_type", "bus_type"])

        # Start time of the recording
        recordingStartTime = mdf.header.start_time

        for signals in mdf.iter_channels():            

            try:
                numericSignals = signals.samples.astype(np.double)
                stringSignals = np.empty(len(signals.timestamps), dtype=str)
                importType = "numeric"
            except:
                numericSignals = np.full(len(signals.timestamps), dtype=np.double, fill_value=0)
                stringSignals = signals.samples.astype(str)
                importType = "string"

            logging.info(f"Exporting signal: {signals.name} as type {importType}")               

            for indx in range(0, len(signals.timestamps)):

                try:
                    numericValue = float(signals.samples[indx])
                except:
                    numericValue = "",

                writer.writerow(
                    [
                        target_file,
                        str(csv_file).rsplit('/')[-1],
                        signals.name, 
                        signals.unit, 
                        signals.timestamps[indx],
                        recordingStartTime + timedelta(seconds=signals.timestamps[indx]),
                        numericSignals[indx],
                        stringSignals[indx],
                        signals.source.source_type,
                        signals.source.bus_type
                    ]
                )

    csvFile.close()



def main(event: func.EventGridEvent):
    
    #boilerplated event trigger code
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

    # create temporary directory and file to placehold downloaded blob
    try:
        tempFilePath = tempfile.gettempdir()
        mdf_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('mdf file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')
        raise


    # Acquire a credential object
    credential = DefaultAzureCredential()

    #download from blob into local file
    blob_service_client_instance = BlobServiceClient(account_url=raw_STORAGEACCOUNTURL, credential=credential)
    try:
        blob_client_instance = blob_service_client_instance.get_blob_client(container=raw_CONTAINERNAME, blob=raw_BLOBNAME, snapshot=None)
    except ResourceNotFoundError as e:
       logging.info("No blob found.")
       raise

    t1=time.time()
    with open(mdf_LOCALFILE.name, "wb") as mf4_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(mf4_blob)
    t2=time.time()
    logging.info(("It takes %s seconds to download mdf file: "+raw_BLOBNAME) % (t2 - t1))

    # parse mf4 blob
    mdf = MDF(mdf_LOCALFILE.name)
    logging.info('mdf version 4 parsed successfully')

    
    #curated blob details
    curated_CONTAINERNAME= raw_CONTAINERNAME.replace('raw' , 'curated')
    curated_BLOBNAME= raw_BLOBNAME.replace('.mf4' , '-signalscsv.gz')


    # create temporary directory and file to placehold downloaded blob
    try:
        tempFilePath = tempfile.gettempdir()
        csv_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('csv file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')
        raise

    writeCsv(csv_LOCALFILE.name, mdf, curated_BLOBNAME)

    logging.info("CSV file created successfully")

    # Upload csv file to curated container
    try:
        curated_blob_client_instance = blob_service_client_instance.get_blob_client(container=curated_CONTAINERNAME, blob=curated_BLOBNAME)
    except ResourceNotFoundError as e:
       logging.info("No blob found.")
       raise
    logging.info("\nUploading to Azure Storage as blob:\n\t" + curated_BLOBNAME)
    # Upload the created file
    with open(file=csv_LOCALFILE.name, mode="rb") as data:
        curated_blob_client_instance.upload_blob(data)   
