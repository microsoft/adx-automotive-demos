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

from MDF2AnalyticsFormatProcessing import processSignals
from DecodeParquet import processSignalAsParquet
from DecodeCSV import processSignalAsCsv
from MetadataTools import calculateMetadata, writeMetadata, dumpSignals

# This implementation just sends the result to the console
def log_result(result):
    print(f"{result}")

# This implementation just sends the result to the console
def log_error(error):
    print(f"{error}")

# This implementation just sends completition status to the console on 10% increments
updates = 0

def log_completition(result):
    global updates

    if divmod(int(result), 10) == (updates, 0):
        updates += 1
        print(f"Completed {result:9.0f}%")
    
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

        # Reads the MDF file metadata with all the file and signal information
        metadata = calculateMetadata(filename, basename, file_uuid)
        signalsMetadata = metadata["signals"]
        numberOfSignals = len(signalsMetadata)

        # Use the right method based on the format
        if (args.exportFormat == "parquet"):         
            processSignals(filename, basename, file_uuid, args.target, signalsMetadata, readBlacklistedSignals(), processSignalAsParquet, numberOfSignals, log_result, log_error, log_completition, createReport)
        elif (args.exportFormat == "csv"):         
            processSignals(filename, basename, file_uuid, args.target, signalsMetadata, readBlacklistedSignals(), processSignalAsCsv, numberOfSignals, log_result, log_error, log_completition, createReport)
        else:
            print("Incorrect format selected, use argument --format with parquet or csv")     

        # Writes the calculated metadata
        writeMetadata(metadata, filename, basename, args.target)           

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


