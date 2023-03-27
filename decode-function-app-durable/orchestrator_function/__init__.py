# This function is not intended to be invoked directly. Instead it will be
# triggered by an Event Trigger function.
# Before running this sample, please:
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt


import logging
import json

import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):

    # Get input details
    storage_blob_details_input = context.get_input()
    if storage_blob_details_input is None:
        raise Exception("Orchestrator: Details of the blob event trigger are required as input")
    logging.info(f"Orchestrator: orchestrator has been called with details as: {storage_blob_details_input}")

    # Parse input details
    try:
        storage_blob_details= json.loads(storage_blob_details_input)
        raw_BLOBNAME = storage_blob_details['raw_BLOBNAME']

    except Exception as e:
        logging.info(f"Orchestrator: Failed to parse input details for the durable orchestrator, {e}")
        raise
    

    ######################### Orchestrator Step-1: download & chunk mdf file #########################
    logging.info(f"Orchestrator: ###### Orchestrator Step-1 initiated: download and chunk '{raw_BLOBNAME}' MDF file ######")
    # Call download activity function to download the main MDF file and chunk into small MDF files for each signal
    try:
        mdf_chunks_list = yield context.call_activity("download_activity_function", storage_blob_details)
        logging.info("Orchestrator: ######################### Orchestrator End of: Step-1 ####################################")

    except Exception as e:
        logging.info(f"Orchestrator: Failed to finish download activity for {raw_BLOBNAME}, {e}")
        raise
    ######################### Orchestrator End of: Step-1 ####################################
    




    ######################### Orchestrator Step-2: Decode each signal #########################
    logging.info(f"Orchestrator: ###### Orchestrator Step-2 initiated: decode and generate {raw_BLOBNAME} parquet file(s) ######")
    

    # Create parallel upload activity function on each signal file chunk of the source MDF file.
    tasks = []
    try:
        for signal_file in mdf_chunks_list:
            storage_blob_details.update({
                'signal_file': signal_file
                })
            upload_data = json.dumps(storage_blob_details)
            tasks.append(context.call_activity("upload_activity_function", upload_data))
    
    except Exception as e:
        logging.info(f"Orchestrator: Failed to create parallel upload activities for signal chunk: {signal_file}, {e}")
        raise
    
    logging.info("Orchestrator: ######################### Orchestrator End of: Step-2 ####################################")
    ######################### Orchestrator End of: Step-2 ####################################





    ######################### Step-3: produce metadata file #########################
    ######################### End of: Step-3 ####################################
    # Wait for all parallel tasks to finish execution
    results = yield context.task_all(tasks)
    activity_counter = sum(results)
    return activity_counter


main = df.Orchestrator.create(orchestrator_function)