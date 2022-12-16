import json
import csv
import time
import logging
import tempfile
import gzip
import uuid
import numpy as np
import pandas as pd
from asammdf import MDF
from datetime import datetime, timedelta

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

"""

# Create a parquet file for the MDF
def writeParquet(parquet_file, mdf):    
    mdf.export(fmt="parquet", filename=parquet_file, raw=False, empty_channels="skip", ignore_value2text_conversions = False, time_from_zero=False, compression="GZIP")

"""

# Creates a metadata file for the MDF-4
def writeMetadata(meta_LOCALFILE, metadata_BLOBNAME, mdf, str_file_id, data_file, numberOfChunks , blob_service_client_instance, curated_CONTAINERNAME):

    # initialize signal's meta data
    allSignalMetadata = []
    for signal in mdf.iter_channels():
        allSignalMetadata.append(
            {
                "name": signal.name,
                "comment": signal.comment
            }
        )

    with open(meta_LOCALFILE, 'w') as metadataFile:
        metadata = {
            "name": data_file,
            "source_uuid": str_file_id,
            "preparation_startDate": str(datetime.utcnow()),
            "signals_description": allSignalMetadata,
            "comments": mdf.header.comment,
            "numberOfChunks": str(numberOfChunks + 1)
        }
        metadataFile.write(json.dumps(metadata))

    metadataFile.close()

    # Upload meta data file to curated container
    try:
        metadata_blob_client_instance = blob_service_client_instance.get_blob_client(container=curated_CONTAINERNAME, blob=metadata_BLOBNAME)
        with open(meta_LOCALFILE, mode="rb") as data:
            metadata_blob_client_instance.upload_blob(data)
            logging.info("\nUploading meta data file to Azure Storage as blob:\n\t" + metadata_BLOBNAME)  
    except:
       logging.info("cannot upload new CSV file, file might be already exist: "  + metadata_BLOBNAME)
       raise


# Writes a gzipped CSV file using the uuid as name
def writeCsv(csv_LOCALFILE, curated_base_BLOBNAME, mdf, str_file_id, blob_service_client_instance, curated_CONTAINERNAME):

    # Start time of the recording
    recordingStartTime = mdf.header.start_time

    # Initial counter
    counter = 0

    # Iterate over the signals
    for signals in mdf.iter_channels(): 
        
        # open the file in the write mode
        with gzip.open(csv_LOCALFILE, 'wt') as csvFile:

            writer = csv.writer(csvFile)
            writer.writerow(["source_uuid", "name", "unit", "relativeTimestamp", "absoluteTimestamp", "value", "value_string", "source_type", "bus_type"])

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
                        str_file_id,
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
       
        # Upload csv file to curated container
        chunk_BLONNAME = str(curated_base_BLOBNAME + '-' + str_file_id + '-' + str(counter) + '.csv.gz')
        # upload 4 MB for each request
        chunk_size = 4*1024*1024
        try:
            curated_blob_client_instance = blob_service_client_instance.get_blob_client(container=curated_CONTAINERNAME, blob= chunk_BLONNAME)
            if(curated_blob_client_instance.exists):
                curated_blob_client_instance.delete_blob()
                curated_blob_client_instance.create_append_blob()
            with open(csv_LOCALFILE, mode="rb") as data_stream:
                while True:
                    read_data = data_stream.read(chunk_size)
                    if not read_data:
                        break 
                    curated_blob_client_instance.append_block(read_data)
                # curated_blob_client_instance.upload_blob(data)
            logging.info("\n CSV file to Azure Storage as blob:\n\t" + chunk_BLONNAME)  
        except:
            logging.info("cannot upload new chunk CSV file, file might be already exist: "  + chunk_BLONNAME)
            raise
        
        counter +=1

    return counter 
    



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
    try:
        blob_service_client_instance = BlobServiceClient(account_url=raw_STORAGEACCOUNTURL, credential=credential)
        blob_client_instance = blob_service_client_instance.get_blob_client(container=raw_CONTAINERNAME, blob=raw_BLOBNAME, snapshot=None)
    except:
       logging.info("cannot download the blob")
       raise

    t1=time.time()
    with open(mdf_LOCALFILE.name, "wb") as mf4_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(mf4_blob)
    t2=time.time()
    logging.info(("It takes %s seconds to download mdf file: "+raw_BLOBNAME) % (t2 - t1))

    # parse mf4 blob
    try:
        mdf = MDF(mdf_LOCALFILE.name)
        logging.info('mdf version 4 parsed successfully')
    except:
        logging.info("cannot parse MDF file")
        raise

    
    #curated blob details
    file_id = uuid.uuid4()
    str_file_id = str(file_id)
    curated_CONTAINERNAME= raw_CONTAINERNAME.replace('raw' , 'curated')
    curated_base_BLOBNAME= raw_BLOBNAME.replace('.mf4' , '')
    metadata_BLOBNAME = raw_BLOBNAME.replace('.mf4' , '-' + str_file_id + '.metadata.json')


    # create temporary directory and file to placehold csv blob
    try:
        tempFilePath = tempfile.gettempdir()
        csv_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('csv file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')
        raise


    logging.info("Exporting to: " + curated_base_BLOBNAME + '-' + str_file_id + '.csv.gz' )
    numberOfChunks = writeCsv(csv_LOCALFILE.name, curated_base_BLOBNAME, mdf, str_file_id, blob_service_client_instance, curated_CONTAINERNAME)
    logging.info("CSV decoded file uploaded successfully")


     # create temporary directory and file to placehold meta blob
    try:
        tempFilePath = tempfile.gettempdir()
        meta_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('meta data file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')
        raise


    logging.info("Exporting meata data to: " + metadata_BLOBNAME )
    writeMetadata(meta_LOCALFILE.name, metadata_BLOBNAME, mdf, str_file_id, curated_base_BLOBNAME, numberOfChunks , blob_service_client_instance, curated_CONTAINERNAME)
    logging.info("meta data file uploaded successfully")
    
    
     
