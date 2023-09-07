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


def log_result(result):
    print(f"{result}")

def log_error(error):
    print(f"{error}")

def processSignals(filename, basename, uuid, target, signalsMetadata, blacklistedSignals, method):
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
    '''   

    finishedSignals = []
    errorSignals = []
    timeoutSignals = []
    
    targetdir = os.path.join(target, f"{basename}-{uuid}")
    
    try:
        # Create a pool of worker processes with all available CPUs -1
        # To avoid potential conflicts with the MDF library, we will restart the process for each signal (maxtasks per child=1)
        # We also use the spawn context to avoid problems with fork()
        pool = get_context("spawn").Pool(mp.cpu_count()-1, maxtasksperchild=1)
        

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

        # get the task result with a timeout defined as 3 minutes per signal.
        # This is a blocking call, so we will check the results in the order in which the tasks are submitted
        for counter, result in enumerate(results):            

            try:
                print(f"Waiting for value for {counter} - {signalsMetadata[counter]['name']}")
                value = result.get(timeout=60*6) # Wait for the value for 6 minutes, if it is not ready, it will probably never be                    
                finishedSignals.append(
                    {
                        "counter": counter,
                        "name": signalsMetadata[counter]["name"],
                        "value": value
                    }
                )
            except mp.TimeoutError as te:
                print(f"TimeoutError for {counter} - {signalsMetadata[counter]['name']}: {te}")
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
        createReport(basename, target, uuid, finishedSignals, errorSignals, timeoutSignals)
        pool.terminate()


def createReport(basename, target, uuid, finishedSignals, errorSignals, timeoutSignals):
    '''
         Write a JSON file with the results
    '''
    with open(os.path.join(target, f"{basename}-{uuid}.report.json"), 'w') as reportFile:
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

def processFile(filename):
    '''Processes a single MDF file.'''

    start_time = time.time()
    numberOfSignals = 0

    # If the argument is dump, show the signals
    if (args.dump):
       numberOfSignals = dumpSignals(filename)
        
    # Otherwise export to the desired format
    else:        
        basename = Path(filename).stem
        file_uuid = uuid.uuid4()          

        # Write a metadata file with all the file and signal information
        signalsMetadata = writeMetadata(filename, basename, file_uuid, args.target)        
        numberOfSignals = len(signalsMetadata)

        # Use the right method based on the format
        if (args.exportFormat == "parquet"):         
            processSignals(filename, basename, file_uuid, args.target, signalsMetadata, readBlacklistedSignals(), processSignalAsParquet)
        elif (args.exportFormat == "csv"):         
            processSignals(filename, basename, file_uuid, args.target, signalsMetadata, readBlacklistedSignals(), processSignalAsCsv)
        else:
            print("Incorrect format selected, use argument --format with parquet or csv")                

    end_time = time.time() - start_time
    print (f"Processing {filename} took {end_time} and has {numberOfSignals} signals")

def processDirectory(directoryname):
    '''Processes a complete directoy containing several MDF-4 files.'''
    for path in os.listdir(directoryname):
        if os.path.isfile(os.path.join(directoryname, path)):
            processFile(os.path.join(directoryname, path))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single MDF-4 or directory with MDF-4 files into CSV or Parquet Files.")
    parser.add_argument("-f", "--file", dest="file", help="Path to a single MDF-4 file.")
    parser.add_argument("-d", "--directory", dest="directory", help="Path to a directory with MDF-4 files.")
    parser.add_argument("-t", "--target", dest="target", default=".", help="Location where the processed files will be stored.")
    parser.add_argument("--dump", dest="dump", action="store_true", help="Shows the signals contained in the file. No export will be made.")
    parser.add_argument("--format", dest="exportFormat", default="parquet", help="Use csv or parquet to select your export format. Default is parquet")
    args = parser.parse_args()

    if(args.file):
        processFile(args.file)
    elif(args.directory):
        processDirectory(args.directory)
