# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import time
import logging
import tempfile
import os
import json
import uuid
from datetime import datetime, timedelta
from asammdf import MDF, Signal
from asammdf.blocks import v4_constants as v4c

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

def main(downloadInput) -> list[str]:

    # Parse input data
    try:
        account_url = downloadInput["STORAGEACCOUNTURL"]
        raw_container = downloadInput["raw_CONTAINERNAME"]

        # Get the raw file name and the base (without extension)
        raw_blob_name = downloadInput["raw_BLOBNAME"]
        intermediate_base_BLOBNAME= raw_blob_name.split('.')[0]

        intermediate_CONTAINERNAME = downloadInput['intermediate_CONTAINERNAME']
        unique_id = downloadInput['unique_id']

        curated_CONTAINERNAME = downloadInput['curated_CONTAINERNAME']
    except Exception as e:
        logging.info(f"Download Activity: Failed to parse input data, {e}")
        raise
    
    # Create temporary directory and file to placehold downloaded blob
    try:
        mdf_tempFilePath = tempfile.gettempdir()
        mdf_LOCALFILE = tempfile.NamedTemporaryFile()
        meta_LOCALFILE = tempfile.NamedTemporaryFile()
        mdf_signal_LOCALFILE = tempfile.NamedTemporaryFile()
        logging.info(f'Download Activity: mdf files created locally \n main MDF file: {mdf_LOCALFILE} and signals file: {mdf_signal_LOCALFILE}')

    except FileNotFoundError as e:
        logging.info(f'Download Activity: Failed to create directory, {e}')
        raise

    # Activity step-1: Download from blob into local file

    # Acquire a credential object
    credential = DefaultAzureCredential()

    # Create storage blob service instance and download the source MDF file
    try:
        blob_service_client_instance = BlobServiceClient(account_url = account_url, credential=credential)
        blob_client_instance = blob_service_client_instance.get_blob_client(container=raw_container, blob=raw_blob_name, snapshot=None)
    except Exception as e:
       logging.info(f"Download Activity: Failed to access the storage account, {e}")
       raise

    try:
        t1=time.time()
        with open(mdf_LOCALFILE.name, "wb") as mf4_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(mf4_blob)
        t2=time.time()
        logging.info(("Download Activity: It takes %s seconds to download mdf file: " + raw_blob_name) % (t2 - t1))
        logging.info(f"Download Activity: ###### Step-1 completed sucessfully: raw MDF file downloaded locally as '{mdf_LOCALFILE.name}' ######")
    
    except Exception as e:
       logging.info(f"Download Activity: Failed to download MDF file: {raw_blob_name}, {e}")
       raise   
    
    
    # Load the source MDF file
    try:
        mdf_file = MDF(mdf_LOCALFILE.name)
        logging.info('Download Activity: MDF file loaded successfully')
    except Exception as e:
        logging.info(f"Download Activity: Failed to load MDF file: {raw_blob_name}, {e}")
        raise  
    

    # chunk the MDF file into smaller signal files
    mdf_chunks_list = []
    signals_metadata = []
    total_signals_size = 0
    # Iterate over the signals
    for counter, signal in enumerate(mdf_file.iter_channels()):
        # Load signal file
        try:
            # create empty MDf version 4.00 file
            with MDF(version='4.10') as signal_mdf4:
                # append the signal to the new file
                signal_mdf4.append(signal)
                # save new file
                temp_mdf_file = mdf_signal_LOCALFILE.name + '.mf4'
                with open(temp_mdf_file, "wb") as signal_mdf4_file:
                    signal_mdf4.save(temp_mdf_file, overwrite=True)
                signal_mdf4_file.close()
                temp_file_size = os.stat(temp_mdf_file).st_size
                logging.info(f"Download Activity: intermediate MDF file: {temp_mdf_file} with size {temp_file_size} bytes created for signal: {signal.name}")
                total_signals_size += temp_file_size
            # Flush the signal MDF file    
        except Exception as e:
            logging.info(f"Download Activity: Failed to create intermediate MDF file for signal : {signal.name}, {e}")
            raise
        
        # Load metadata file
        try:
            signals_metadata.append(
                {
                    "name": signal.name,
                    "unit": signal.unit,
                    "comment": signal.comment,
                    "group_index": signal.group_index,
                    "channel_index": signal.channel_index,
                    "group_name": mdf_file.groups[signal.group_index].channel_group.acq_name,
                    "source" : signal.source.name,
                    "source_type": v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type],
                    "bus_type": v4c.BUS_TYPE_TO_STRING[signal.source.bus_type],
                }          
            )
        except Exception as e:
            logging.info(f"Download Activity: Failed to create metadata for signal: {signal.name}")


        # upload intermediate signal mdf file
        try:
            if signal.source.name is not None:
                intermediate_BLOBNAME = intermediate_base_BLOBNAME + '-'  + unique_id + '/signal_name=' + str(signal.name) + '_group_index=' + str(signal.group_index) + '_channel_index=' + str(signal.channel_index) + '_source=' + str(signal.source.name)  + '/' + unique_id + '.mf4'
            else:
                intermediate_BLOBNAME = intermediate_base_BLOBNAME + '-'  + unique_id + '/signal_name=' + str(signal.name) + '_group_index=' + str(signal.group_index) + '_channel_index=' + str(signal.channel_index) + '/' + unique_id + '.mf4'
        except:
            # create unique ID for this signal
            signal_unique_diff = uuid.uuid4()
            intermediate_BLOBNAME = intermediate_base_BLOBNAME + '-' + unique_id + '/signal_name=' + str(signal.name) + '_group_index=' + str(signal.group_index) + '_channel_index=' + str(signal.channel_index) + '_signal_uniqueness=' + str(signal_unique_diff)  + '/' + unique_id + '.mf4' 

        try:
            intermediate_blob_client_instance = blob_service_client_instance.get_blob_client(container=intermediate_CONTAINERNAME, blob=intermediate_BLOBNAME)
            with open(temp_mdf_file, mode="rb") as signal_data:
                intermediate_blob_client_instance.upload_blob(data = signal_data)

        except Exception as e:
            logging.info(f"Download Activity: Failed to upload intermediate MDF file for signal : {signal.name}, {e}")
            raise
        
       
        logging.info(f"Download Activity: intermediate MDF file: Signal {counter}: {signal.name} with {len(signal.timestamps)} uploaded to Azure Storage as blob:\n\t" + intermediate_BLOBNAME)
        mdf_chunks_list.append(intermediate_BLOBNAME)

    ###### Meta data Section #####
    # Initialize meta-data
    metadata = {
            "name": intermediate_base_BLOBNAME,
            "source_uuid": unique_id,
            "preparation_startDate": str(datetime.utcnow()),
            "signals": signals_metadata,
            "comments": mdf_file.header.comment,
            "numberOfChunks": counter
        }
    # Save source file meta-data
    metadata_BLOBNAME = intermediate_base_BLOBNAME+ '-' + unique_id + '.metadata.json'
    with open(meta_LOCALFILE.name, 'w') as metadataFile:
        metadataFile.write(json.dumps(metadata))
    metadataFile.close()
    
    # Upload meta data file to curated container
    try:
        metadata_blob_client_instance = blob_service_client_instance.get_blob_client(container=curated_CONTAINERNAME, blob=metadata_BLOBNAME)
        with open(meta_LOCALFILE.name, mode="rb") as data:
            metadata_blob_client_instance.upload_blob(data)
            logging.info(f"Download Activity: meta data file: {metadata_BLOBNAME} uploaded to container: {curated_CONTAINERNAME}")  
    except Exception as e:
       logging.info(f"Download Activity: failed to create metadata file: {metadata_BLOBNAME}")
       raise

    logging.info(f"Download Activity: Total size of signal chunks = {total_signals_size}")        
    return mdf_chunks_list
