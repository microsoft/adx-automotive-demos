# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import argparse
import json
import os
from   pathlib import Path
import time 
import multiprocessing as mp
from multiprocessing import get_context
import uuid
from DecodeParquet import processSignalAsParquet
from DecodeCSV import processSignalAsCsv
from MetadataTools import writeMetadata, dumpSignals


def processSignals(filename, basename, uuid, target, signalsMetadata, blacklistedSignals, method, numberOfSignals, log_result, log_error, log_completition, createReport):
    '''
        Writes the MDF-4 file to a file that can be used by ADX.
        Each signal will be processed in parallel.
        
        Args:
            filename: the MDF-4 file to process
            basename: the base name of the metadata file
            uuid: the UUID that identifies this decoding run
            target: the target directory where to write the metadata file
            signalsMetadata: the list of signals to process
            blacklistedSignals: the list of signals to skip
            method: the method to use to process the signals
            log_result: the callback to use when a signal is processed successfully
            log_error: the callback to use when a signal is processed with an error
    '''   

    finishedSignals = []
    errorSignals = []
    timeoutSignals = []
    
    targetdir = os.path.join(target, f"{basename}-{uuid}")
    
    try:
        # Create a pool of worker processes with all available CPUs -1
        # To avoid potential memory leaks with the MDF library, we will restart the process after a certain number of processes (maxtasks per child)
        # We also use the spawn context to avoid problems with fork()
        pool = get_context("spawn").Pool(mp.cpu_count()-1, maxtasksperchild=10)
        

        # Iterate over the signals contained in the MDF-4 file.
        # We will apply the method given as an argument with the callback for both success and error.
        results = []
        for counter, signalMetadata in enumerate(signalsMetadata):

            # Apply the processSignal function to each signal asynchronously
            result = pool.apply_async(
                method, 
                args=(counter, filename, signalMetadata, uuid, targetdir, blacklistedSignals),
                callback=log_result,
                error_callback=log_error
            )
            results.append(result)

        # All tasks have been submitted, no more tasks will be added to this pool.
        pool.close()

        vEntriesCount = 0 # Capture TOTAL( no. of entries per signal )
        # get the task result with a timeout defined as 3 minutes per signal.
        # This is a blocking call, so we will check the results in the order in which the tasks are submitted
        for counter, result in enumerate(results):            



            try:
                #print(f"Waiting for value for {counter} - {signalsMetadata[counter]['name']}")
                value = result.get(timeout=60*6) # Wait for the value for 6 minutes, if it is not ready, it will probably never be   

                #Append the value to signalsMetadata json file
                signalsMetadata[counter]["signal_decoded_status"] = value[1]
                signalsMetadata[counter]["records_count"] = value[4]
                signalsMetadata[counter]["message"] = value[3]
                
                finishedSignals.append(
                    {
                        "counter": counter,
                        "name": signalsMetadata[counter]["name"],
                        "value": value
                    }
                )

                # Capture finishedSignals with no errors, i.e. with 'True' from Decoding so we can add it to the total entries counts:      
                vEntriesCount = vEntriesCount + value[4]
                    
                log_completition( (len(finishedSignals) / numberOfSignals)*100 ) # Log the percentage of signals processed
                
            except mp.TimeoutError as te:
                print(f"TimeoutError for {counter} - {signalsMetadata[counter]['name']}: {te}")

                signalsMetadata[counter]["signal_decoded_status"] = False
                signalsMetadata[counter]["records_count"] = 0
                signalsMetadata[counter]["message"] = f"TimeoutError {te}"

                timeoutSignals.append(
                    {
                        "counter": counter,
                        "name": signalsMetadata[counter]["name"],
                        "value": f"TimeoutError {te}"
                    }                    
                )
                continue
            except Exception as e:
                print(f"Exception for {counter} - {signalsMetadata[counter]['name']}: {e}")

                signalsMetadata[counter]["signal_decoded_status"] = False
                signalsMetadata[counter]["records_count"] = 0
                signalsMetadata[counter]["message"] = f"Exception {e}"

                errorSignals.append(
                    {
                        "counter": counter,
                        "name": signalsMetadata[counter]['name'],
                        "value": f"Exception {e}"
                    }               
                ) 
                continue

    except Exception as e:
        print(f"Critical error {e}")
    finally:
        # We create a report that contains all signals.
        print (f"Finished. Tasks total/finished/errors/timeout: {len(signalsMetadata)} / {len(finishedSignals)} / {len(errorSignals)} / {len(timeoutSignals)}")
        print (f"Finished: {finishedSignals}")
        print (f"Errors: {errorSignals}")
        print (f"Timeout signals: {timeoutSignals}")
        print(f'Total Cumulative Signal entries count: {vEntriesCount}')
        createReport(basename, target, uuid, signalsMetadata, finishedSignals, errorSignals, timeoutSignals, vEntriesCount)
        pool.terminate()

