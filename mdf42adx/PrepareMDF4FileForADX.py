# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import argparse
from asammdf import MDF
from asammdf.blocks import v4_constants as v4c
import csv
from datetime import datetime
import gzip
import json
import os
from   pathlib import Path
import time 
import multiprocessing as mp
from multiprocessing import get_context
import uuid
from DecodeParquet import processSignalAsParquet
from DecodeCSV import processSignalAsCsv
from DecodeUtils import getSource


def dumpSignals(filename):
    '''
        Iterates over all signals and prints them to the console. Used for debugging purposes.

        Args:
            filename: the MDF-4 file to process
    
    '''

    mdf = MDF(filename)

    for counter, signal in enumerate(mdf.iter_channels()):
        channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, signal)
        print(f"{signal.source.name}, {channel_group_acq_name}, {acq_source_name}, {acq_source_path}, {signal.name}, {signal.unit}, {v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type]}, {v4c.BUS_TYPE_TO_STRING[signal.source.bus_type]}, {signal.group_index}, {signal.channel_index}")

    mdf.close()
    del mdf

    return counter

def writeMetadata(filename, basename, uuid, target):
    '''
        Creates an object containing the signals description for the MDF-4 file and writes it as a JSON file.
        It returns an array with the signal description for further processing.        

        Args:
            filename: the MDF-4 file to process
            basename: the base name of the metadata file
            uuid: the UUID that identifies this decoding run
            target: the target directory where to write the metadata file
        Returns:
            the number of signals in the file
    '''

    mdf = MDF(filename)

    with open(os.path.join(target, f"{basename}-{uuid}.metadata.json"), 'w') as metadataFile:
        
        print(f"Writing metadata file {basename}-{uuid}")

        signalNames = []

        metadata = {
            "name": basename,
            "source_uuid": str(uuid),
            "preparation_startDate": str(datetime.utcnow()),
            "signals": [],
            "comments": mdf.header.comment,
        }

        for signal in mdf.iter_channels():

            channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, signal)

            metadata["signals"].append(
                {
                    "name": signal.name,
                    "unit": signal.unit,
                    "comment": signal.comment,
                    "group_index": signal.group_index,
                    "channel_index": signal.channel_index,
                    "channel_group_acq_name": channel_group_acq_name,
                    "acq_source_name": acq_source_name,
                    "acq_source_path": acq_source_path,
                    "source" : signal.source.name,
                    "source_type": v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type],
                    "bus_type": v4c.BUS_TYPE_TO_STRING[signal.source.bus_type],
                }          
            )
        metadataFile.write(json.dumps(metadata))
       
    print(f"Finished writing metadata file {basename}-{uuid} with {len(metadata['signals'])}")

    mdf.close()

    del mdf

    return metadata["signals"]


def log_result(result):
    print(f"{result}")

def log_error(error):
    print(f"{error}")

def processSignals(filename, basename, uuid, target, signalsMetadata, blacklistedSignals, method):
    '''
        Writes the MDF-4 file to a parquet file.
        Each signal will be written to a separate file in parallel.
        
        Args:
            filename: the MDF-4 file to process
            basename: the base name of the metadata file
            uuid: the UUID that identifies this decoding run
            target: the target directory where to write the metadata file
            signalsMetadata: the list of signals to process
            blacklistedSignals: the list of signals to skip
            method: the method to use to process the signals
    '''   

    okSignals = []
    nokSignals = []
    timeoutSignals = []
    
    targetdir = os.path.join(target, f"{basename}-{uuid}")
    
    try:
        # Create a pool of worker processes with all available CPUs
        pool = get_context("spawn").Pool(mp.cpu_count()-1)

        # Iterate over the signals contained in the MDF-4 file
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
                okSignals.append( (signalsMetadata[counter], value))
            except mp.TimeoutError as te:
                print(f"TimeoutError for {counter} - {signalsMetadata[counter]['name']}: {te}")
                timeoutSignals.append(signalsMetadata[counter])
                continue
            except Exception as e:
                print(f"Exception for {counter} - {signalsMetadata[counter]['name']}: {e}")
                nokSignals.append(signalsMetadata[counter])
                continue

    except Exception as e:
        print(e)
    finally:
        print (f"Finished. Tasks total/finished/exceptions/timeout: {len(signalsMetadata)} / {len(okSignals)} / {len(nokSignals)} / {len(timeoutSignals)}")
        print (f"Finished: {okSignals}")
        print (f"Exceptions: {nokSignals}")
        print (f"Timeout signals: {timeoutSignals}")
        pool.terminate()


def readBlacklistedSignals():
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
