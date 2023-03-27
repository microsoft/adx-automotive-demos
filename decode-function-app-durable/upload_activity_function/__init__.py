# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import json
import csv
import time
import logging
import tempfile
from datetime import datetime, timedelta
import numpy as np
from asammdf import MDF, Signal
from asammdf.blocks import v4_constants as v4c

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


def main(uploadDetails):

    # Parse input data
    try:
        uploadDetailsJSON = json.loads(uploadDetails)
        
        curated_STORAGEACCOUNTURL = uploadDetailsJSON["STORAGEACCOUNTURL"]
        curated_CONTAINERNAME = uploadDetailsJSON["curated_CONTAINERNAME"]
        
        # Get the raw file name and the base (without extension)
        raw_blob_name = uploadDetailsJSON["raw_BLOBNAME"]
        curated_base_BLOBNAME= raw_blob_name.split('.')[0]

        intermediate_CONTAINERNAME = uploadDetailsJSON['intermediate_CONTAINERNAME']
        unique_id = uploadDetailsJSON["unique_id"]
        signal_file: str = uploadDetailsJSON["signal_file"]

    except Exception as e:
        logging.info(f"Upload Activity: Failed to parse input data, {e}")
        raise

    # Create temporary directory and file to placehold downloaded blob
    try:
        mdf_tempFilePath = tempfile.gettempdir()
        mdf_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info('Upload Activity: intermediate signal mdf file created locally')

    except FileNotFoundError as e:
        logging.info('Upload Activity: cannot create directory')
        raise

    # Download intermediate signal MDF file
    try:
        # Acquire a credential object
        credential = DefaultAzureCredential()

        # Create storage blob service instance
        blob_service_client_instance = BlobServiceClient(account_url = curated_STORAGEACCOUNTURL, credential=credential)
        blob_client_instance = blob_service_client_instance.get_blob_client(container=intermediate_CONTAINERNAME, blob=signal_file, snapshot=None)
    except:
       logging.info("Upload Activity: cannot access the storage account")
       raise

     # Download intermediate signal MDF file
    try:
        t1=time.time()
        with open(mdf_LOCALFILE.name, "wb") as mf4_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(mf4_blob)
        t2=time.time()
        logging.info(("Upload Activity: It takes %s seconds to download intermediate signal mdf file: "+ signal_file) % (t2 - t1))
    
    except Exception as e:
       logging.info(f"Upload Activity: Failed to download intermediate signal MDF file: {signal_file}, {e}")
       raise   
    
    # load mf4 file
    try:
        signal_mdf = MDF(mdf_LOCALFILE.name)
        logging.info('Upload Activity: intermediate signal mdf file loaded successfully')
    except:
        logging.info("Upload Activity: cannot load MDF file")
        raise

    # decode the signal and generate parquet files
    writer = None
    start_signal_time = time.time()
    # try:
    #     # Can be HARD CODED to iter_channel()[0] -> if one signal per file
    #     # for future, if it could be more than one signal, for loop is needed i.e. for signal in signal_mdf.iter_channel():
    #     signal: Signal = signal_mdf.iter_channels()[0]
    #     logging.info(f"Upload Activity: Signal: {signal.name} with {len(signal.timestamps)} loaded successfully")
    # except Exception as e:
    #     logging.info(f"Upload Activity: Signal data failed to load: {e}")
    #     raise

    # Iterate over the signals
    for counter, signal in enumerate(signal_mdf.iter_channels()):

        try:
            if (signal.samples.dtype == int or signal.samples.dtype == float):
                numericSignals = signal.samples.astype(np.double)
                stringSignals = np.empty(len(signal.timestamps), dtype=str)
            elif (signal.smamples.dtype == str or signal.samples.dtype == object):
                numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)
                stringSignals = signal.samples.astype(str)
            elif (signal.samples.dtype == complex):
                print(f"Signal: {signal.name} with {len(signal.timestamps)} is a complex type")
                numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)
                stringSignals = np.empty(len(signal.timestamps), dtype=str)
        except:
            # print(f"Signal {counter}: {signal.name} with {len(signal.timestamps)} failed: {e}")

            # In case of error provide a message and leave the signal blank 
            numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)
            stringSignals = np.empty(len(signal.timestamps), dtype=str)     
        try:
            table = pa.table (
                {                   
                    "source_uuid": np.full(len(signal.timestamps), unique_id, dtype=object),
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
        except Exception as e:
            logging.info(f"Upload Activity: Signal: {signal.name} with {len(signal.timestamps)} failed: {e}")

        
        # create temporary directory and file to placehold parquet blob
        try:
            parquet_tempFilePath = tempfile.gettempdir()
            parquet_LOCALFILE = tempfile.NamedTemporaryFile()
            # parquet_rootpath = str(parquet_tempFilePath) + '/' + parquet_LOCALFILE.name + '/'
            logging.info('Upload Activity: parquet file created locally')

        except FileNotFoundError as e:
            logging.info('Upload Activity: cannot create directory')
            raise 
        
        try:
            # Create the writer for the parquet file and write the table
            if writer is None:
                writer = pq.ParquetWriter(parquet_LOCALFILE.name, table.schema, compression="SNAPPY")
            # pq.write_to_dataset(table, root_path=parquet_tempFilePath, partition_cols=["name"])
            writer.write_table(table)
            writer.close()
        except Exception as e:
            logging.info(f"Upload Activity: Signal: {signal.name} parquet file generation failed: {e}")

        
        # Upload parquet file to curated container
        # Initialize parquet file details
        # create unique file name

        try:
            curated_BLOBNAME = signal_file.replace('.mf4' , '.parquet')
        except Exception as e:
            logging.info(f"Upload Activity: Failed to create curated blob name: {curated_BLOBNAME}, Error in .mf4 name {e}")

        try:
            curated_blob_client_instance = blob_service_client_instance.get_blob_client(container=curated_CONTAINERNAME, blob=curated_BLOBNAME)
            with open(parquet_LOCALFILE.name, mode="rb") as parquet_data:
                curated_blob_client_instance.upload_blob(data = parquet_data)
            logging.info("Upload Activity: parquet file to Azure Storage as blob:\n\t" + curated_BLOBNAME)  
        except Exception as e:
            logging.info(f"Upload Activity: cannot upload new parquet file, file might be already exist: {curated_BLOBNAME}, {e}")
            raise
        end_signal_time = time.time() - start_signal_time
        logging.info(f"Upload Activity: Signal: {signal.name} with {len(signal.timestamps)} entries took {end_signal_time}")
        
        parquet_LOCALFILE.flush()
    return counter 
