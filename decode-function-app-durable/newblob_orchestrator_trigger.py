import json
import logging
import uuid

import azure.functions as func
import azure.durable_functions as df

async def main(event: func.EventGridEvent, starter: str):

    # Boilerplated code of the event grid trigger function
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    
    try:
        # Parse event data
        event_url= str(event.get_json()['url'])
        STORAGEACCOUNTURL= event_url.rsplit('/', 2)[0]
        raw_CONTAINERNAME= event_url.rsplit('/', 2)[1]
        raw_BLOBNAME= event_url.rsplit('/', 2)[2]

    except Exception as e:
        logging.info(f"Failed to parse event data, {e}")
        raise



    try:
        # Create unique ID for this function execution
        unique_id = uuid.uuid4()
        str_unique_id = str(unique_id)

        # **HARD CODED** Assume target container to hold intermediate and curated outputs
        curated_CONTAINERNAME= raw_CONTAINERNAME.replace('raw' , 'curated')
        intermediate_CONTAINERNAME = raw_CONTAINERNAME.replace('raw' , 'intermediate')
    
    except Exception as e:
        logging.info(f"Failed to generate extra parameters for durable function, {e}")
        raise

    try:
        # Create Durable function input parameter as a JSON object 
        storage_blob_details = json.dumps({
        'STORAGEACCOUNTURL': STORAGEACCOUNTURL,
        'raw_CONTAINERNAME': raw_CONTAINERNAME,
        'raw_BLOBNAME': raw_BLOBNAME,
        'intermediate_CONTAINERNAME': intermediate_CONTAINERNAME,
        'curated_CONTAINERNAME': curated_CONTAINERNAME,
        'unique_id': str_unique_id
        })
    except Exception as e:
        logging.info(f"Failed to create Input parameter for durable function, {e}")
        raise   

    try:    
        # Start the Durable Functions orchestrator
        client = df.DurableOrchestrationClient(starter)
        logging.info(f"Initiating orchestration with \n Storage: '{STORAGEACCOUNTURL}'\n Container: '{raw_CONTAINERNAME}'\n Blob: '{raw_BLOBNAME}'\n and Unique ID: '{str_unique_id}'\n")
        instance_id = await client.start_new("orchestrator_function", None , storage_blob_details)
        logging.info(f"Started orchestration ID = '{instance_id}'.")
    
    except Exception as e:
        logging.info(f"Failed to start orchestration for, {e}")
        raise

