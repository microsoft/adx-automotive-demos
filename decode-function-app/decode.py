import json
import csv
import time
import logging
import tempfile
import gzip
import uuid
from datetime import datetime, timedelta
import numpy as np
from asammdf import MDF
from asammdf.blocks import v4_constants as v4c

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


# Creates a metadata file for the MDF-4
def writeMetadata(numberOfChunks, meta_LOCALFILE, metadata_BLOBNAME, mdf, str_file_id, data_file , blob_service_client_instance, curated_CONTAINERNAME):

    with open(meta_LOCALFILE, 'w') as metadataFile:
        metadata = {
            "name": data_file,
            "source_uuid": str_file_id,
            "preparation_startDate": str(datetime.utcnow()),
            "signals": [],
            "comments": mdf.header.comment,
            "numberOfChunks": numberOfChunks
        }

        for signal in mdf.iter_channels():
            metadata["signals"].append(
                {
                    "name": signal.name,
                    "unit": signal.unit,
                    "comment": signal.comment,
                    "group_index": signal.group_index,
                    "channel_index": signal.channel_index,
                    "group_name": mdf.groups[signal.group_index].channel_group.acq_name,
                    "source" : signal.source.name,
                    "source_type": v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type],
                    "bus_type": v4c.BUS_TYPE_TO_STRING[signal.source.bus_type],
                }          
            )

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


# Creates and Writes parquet file from mdf
def writeParquet(parquet_tempFilePath, parquet_LOCALFILE, curated_base_BLOBNAME, mdf, str_file_id, blob_service_client_instance, curated_CONTAINERNAME):
    

    # Iterate over the signals
    for counter, signal in enumerate(mdf.iter_channels()):

        writer = None            
        start_signal_time = time.time()

        try:
            numericSignals = signal.samples.astype(np.double)
            stringSignals = np.empty(len(signal.timestamps), dtype=str)            
        except:
            numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)            
            stringSignals = signal.samples.astype(str)       

        try:
            table = pa.table (
                {                   
                    "source_uuid": np.full(len(signal.timestamps), str_file_id, dtype=object),
                    "name": np.full(len(signal.timestamps), signal.name, dtype=object),
                    "unit": np.full(len(signal.timestamps), signal.unit, dtype=object),
                    "timestamp": signal.timestamps,
                    "value": numericSignals,
                    "value_string": stringSignals,
                    "source": np.full(len(signal.timestamps), signal.source.name, dtype=object),
                    "source_type": np.full(len(signal.timestamps), v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type], dtype=object),
                    "bus_type": np.full(len(signal.timestamps), v4c.BUS_TYPE_TO_STRING[signal.source.bus_type], dtype=object)
                }
            ) 

            # Create the writer for the parquet file and write the table
            if writer is None:
                writer = pq.ParquetWriter(parquet_LOCALFILE, table.schema, compression="SNAPPY")
            
            # pq.write_to_dataset(table, root_path=parquet_tempFilePath, partition_cols=["name"])
            writer.write_table(table)
            writer.close()

        except Exception as e:
            print(f"Signal {counter}: {signal.name} with {len(signal.timestamps)} failed: {e}")

        # Upload parquet file to curated container
        try:
            curated_BLONNAME = str(curated_base_BLOBNAME + '-' + str_file_id + '/signal_name=' + str(signal.name) + '/' + str_file_id + '.parquet')
            curated_blob_client_instance = blob_service_client_instance.get_blob_client(container=curated_CONTAINERNAME, blob=curated_BLONNAME)
            with open(parquet_LOCALFILE, mode="rb") as parquet_data:
                curated_blob_client_instance.upload_blob(data = parquet_data)
            logging.info("\n parquet file to Azure Storage as blob:\n\t" + curated_BLONNAME)  
        except:
            logging.info("cannot upload new parquet file, file might be already exist: "  + curated_BLONNAME)
            raise    

        end_signal_time = time.time() - start_signal_time
        print(f"Signal {counter}: {signal.name} with {len(signal.timestamps)} entries took {end_signal_time}")

       
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
        # mdf_tempFilePath = tempfile.gettempdir()
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

    # load mf4 file
    try:
        mdf = MDF(mdf_LOCALFILE.name)
        logging.info('mdf file loaded successfully')
    except:
        logging.info("cannot load MDF file")
        raise

    
    #curated blob details
    file_id = uuid.uuid4()
    str_file_id = str(file_id)
    curated_CONTAINERNAME= raw_CONTAINERNAME.replace('raw' , 'curated')
    curated_base_BLOBNAME= raw_BLOBNAME.replace('.mf4' , '')
    metadata_BLOBNAME = raw_BLOBNAME.replace('.mf4' , '-' + str_file_id + '.metadata.json')


    # create temporary directory and file to placehold parquet blob
    try:
        parquet_tempFilePath = tempfile.gettempdir()
        parquet_LOCALFILE = tempfile.NamedTemporaryFile()
        # parquet_rootpath = str(parquet_tempFilePath) + '/' + parquet_LOCALFILE.name + '/'
        logging.info('parquet file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')
        raise


    logging.info("Exporting to: " + curated_base_BLOBNAME + '-' + str_file_id + '.parquet' )
    numberOfChunks = writeParquet(parquet_tempFilePath, parquet_LOCALFILE.name, curated_base_BLOBNAME, mdf, str_file_id, blob_service_client_instance, curated_CONTAINERNAME)
    logging.info("parquet file uploaded successfully")


     # create temporary directory and file to placehold meta blob
    try:
        # meta_tempFilePath = tempfile.gettempdir()
        meta_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('meta data file created locally')

    except FileNotFoundError as e:
        logging.info('cannot create directory')
        raise


    logging.info("Exporting meata data to: " + metadata_BLOBNAME )
    writeMetadata(numberOfChunks, meta_LOCALFILE.name, metadata_BLOBNAME, mdf, str_file_id, curated_base_BLOBNAME , blob_service_client_instance, curated_CONTAINERNAME)
    logging.info("metadata file uploaded successfully")
    
    
     
