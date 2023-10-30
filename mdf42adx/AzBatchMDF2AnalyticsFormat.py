# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from asammdf import MDF
import json
import os
from   pathlib import Path
import time 
import uuid
import multiprocessing as mp
from multiprocessing import get_context

# Other external script definitions to run this orchestrator script:
from DecodeParquet import processSignalAsParquet
from MetadataTools import writeMetadata
from MDF2AnalyticsFormatProcessing import processSignals

# Dedicated Azure Batch Script:
from AzureBatch import AzureBatchEnvironmentVariables, AzureBatchProcessFilesOutputFolder

# Monitor VM/Batch Pool Statistics:
import psutil
import shutil


# Log the VM Statistics that can aid users to understand the performance on the VM and any hardware bottlenecks, e.g. out of memory issues
def log_hardwareInfo():
    memory_usage = psutil.virtual_memory()
    memory_percent = memory_usage.percent
    cpu_percent = psutil.cpu_percent(interval=1)
    total, used, free = shutil.disk_usage("/")

    hardwareInfoLog = f"***Memory Usage: {memory_percent:.2f}%\nCPU Usage: {cpu_percent:.2f}%\nDisk Space: Total: {total / (2**30):.2f} GB, Used: {used / (2**30):.2f} GB, Free: {free / (2**30):.2f} GB***"
    print(hardwareInfoLog)
    


def log_result(result):
    print(f"{result}")

def log_error(error):
    print(f"{error}")


updates = 0
vPreviousValue = -1 # Ensures that only one event is sent out every 10th percentage - to stop spamming with events

def log_completition(result):
    global updates, vPreviousValue

    #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    ''' Below only works when % completition is exactly 10 %, 20 %, 30 % etc.
    if divmod(int(result), 10) == (updates, 0):
        updates += 1
        print(f"Completed {result:9.0f}%")
    '''
    #-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    '''
        Below ensure that when a threshold is reached (one block of an if statement), one event is pushed/printed e.g. to Azure Event Grid. This ensures we are not spamming eventGrid with multiple events, e.g.
        if there is a file with lots of signals we do not want to fire an event for 0.0001%, 0.0002%, 0.0003% etc.
    '''
    
    if result == 100 and vPreviousValue < 100:
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 90 and vPreviousValue < 90:
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 80 and vPreviousValue < 80:
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 70 and vPreviousValue < 70:
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 60 and vPreviousValue < 60:
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 50 and vPreviousValue < 50:
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 40 and vPreviousValue < 40:  
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 30 and vPreviousValue < 30: 
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 20 and vPreviousValue < 20: 
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result

    elif result >= 10 and vPreviousValue < 10:   
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result
    
    elif result >= 0 and vPreviousValue < 0:   
        print(f'DECODING PROGRESS {result}%...')
        log_hardwareInfo()
        vPreviousValue = result


# This implementation writes the report to the disk    
def createReport(basename, target, uuid, signalsMetadata, finishedSignals, errorSignals, timeoutSignals, vEntriesCount):
    '''
         Write a JSON file with the results

            Args:
                basename: the base name of the metadata file
                target: the target directory where to write the metadata file
                uuid: the UUID that identifies this decoding run
                finishedSignals: the list of signals that were processed successfully
                errorSignals: the list of signals that were processed with an error
                timeoutSignals: the list of signals that were processed with a timeout

    '''

    # From finish signals, get the okSignals and nokSignals:  
    # Create an array with all of the finished signals that have a value of True in the second position
    okSignals = []
    nokSignals = []

    for i in finishedSignals:
        if (i['value'][1] == True):
            okSignals.append(i['name'])
        elif (i['value'][1] == False):
            nokSignals.append(i['name'])
        else:
            print('Cannot find signal True/False value in finishedSignals.')

    
    # Construct long string to alert UI user top n signals that have failed:
    if len(nokSignals) != 0:
        failedSignalString = ""
        counter = 0

        for i in nokSignals:             
            if counter >= 10:
                break
            
            failedSignalString = failedSignalString +  i + '   '
            counter = counter + 1

        print(f"Failed Signals: {failedSignalString}")
    
    else:
        print("No failed signals")


    # TOTAL no. of entries in the input file *not the no. of the signals but the records count of cumulative signals*
    print(f'Total Entries count: {vEntriesCount}')


    # Write the report
    with open(os.path.join(target, f"REPORT-{basename}.json"), 'w') as reportFile:
        report = {
            "finished": finishedSignals,
            "error": errorSignals,
            "timeout": timeoutSignals
        }
        reportFile.write(json.dumps(report))

    

def readBlacklistedSignals():
    # Future implementation can use this method to return a list of blacklisted signals from a file.
    return [
    ]



# Function to kick start processing (decoding) the file:
def processFile(fileLocation, fileNameEnvVar):
    '''Processes a single MDF file.'''
    start_time = time.time()
    mdf = MDF(fileLocation)
    
    # Save the processed file to a separate folder:   
    outputFolder = AzureBatchProcessFilesOutputFolder(fileNameEnvVar)

    # Decoding processess:
    basename = Path(fileLocation).stem
    file_uuid = uuid.uuid4()  
    numberOfChunks = 0
    
    # Send an event to say ready to start decoding:
    print(f"Ready to start decoding for file {fileLocation}...")

    signalsMetadata = writeMetadata(basename, mdf, file_uuid, outputFolder, fileLocation)
    numberOfSignals = len(signalsMetadata)
    print(f"Total Number of Signals: {numberOfSignals}")
    processSignals(fileLocation, basename, file_uuid, outputFolder, signalsMetadata, readBlacklistedSignals(), processSignalAsParquet, numberOfSignals, log_result, log_error, log_completition, createReport)

    end_time = time.time()

    print (f"Processing {fileLocation} started at {start_time} and ended at {end_time} (took {time.time() - start_time} secs / {(time.time() - start_time)/60} mins) and has {numberOfSignals} signals")

    # Decoding finish:
    print(f"Decoding completed for file {fileLocation}. Processing {fileLocation} took {end_time}")
    print(f"Finishing Batch Task...")


#*******************************************************************************************************************************************************************************
#****
#****                                                               Main Azure Batch Orchestrator Start point
#****
#*******************************************************************************************************************************************************************************
# Main entry point when script is executed:
if __name__ == "__main__":

    try:
        print('Starting up...')
        log_hardwareInfo() # Log the VM Statistics before any decoding happens
        
        taskWorkingDirectory, taskBatchDirectory, taskNodeRootDirectory, fileNameEnvVar = AzureBatchEnvironmentVariables() # Read the Azure Batch Environment variables - external script 

        fileLocation = f"{taskWorkingDirectory}/{fileNameEnvVar}" # Construct the full location of the raw mf4 file on the VM Volume
        processFile(fileLocation, fileNameEnvVar) # Main function for decoding processess of MDF files
        print("File Processed ...")

    except Exception as e:
        print(f"General Error occured for file {fileNameEnvVar}.\n {e}")
        raise

        
    
