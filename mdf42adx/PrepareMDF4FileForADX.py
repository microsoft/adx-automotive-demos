# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import argparse
from asammdf import MDF
import csv
from datetime import datetime, timedelta
import gzip
import json
import os
import pandas as pd
from pathlib import Path
import time
import numpy as np
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from asammdf.blocks import v4_constants as v4c

# Iterates over all signals and prints them to the console
def dumpSignals(basename, mdf, uuid):
       
    for counter, signal in enumerate(mdf.iter_channels()):

        try:
            numericSignals = signal.samples.astype(np.double)
            stringSignals = np.empty(len(signal.timestamps), dtype=str)            
        except:
            numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)            
            stringSignals = signal.samples.astype(str)       

        for indx in range(0, len(signal.timestamps)):                               

           print(
                [
                    str(uuid),
                    signal.name, 
                    signal.unit, 
                    signal.timestamps[indx],
                    numericSignals[indx],            
                    stringSignals[indx],
                    signal.source.name,
                    v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type],
                    v4c.BUS_TYPE_TO_STRING[signal.source.bus_type]
                ]
            )

# Creates a metadata file for the MDF-4
def writeMetadata(basename, mdf, uuid, target, numberOfChunks):

    allSignalMetadata = []
    for signal in mdf.iter_channels():
        allSignalMetadata.append(
            {
                "name": signal.name,
                "comment": signal.comment
            }
        )

    with open(os.path.join(target, f"{basename}-{uuid}.metadata.json"), 'w') as metadataFile:
        
        metadata = {
            "name": basename,
            "source_uuid": str(uuid),
            "preparation_startDate": str(datetime.utcnow()),
            "signals_description": allSignalMetadata,
            "comments": mdf.header.comment,
            "numberOfChunks": numberOfChunks
        }
        metadataFile.write(json.dumps(metadata))
       
    metadataFile.close()

# Create a parquet file for the MDF
def writeParquet(basename, mdf, uuid, target):               

    targetfile = os.path.join(target, f"{basename}-{uuid}.parquet")

    writer = None

    # Iterate over the signals
    for counter, signal in enumerate(mdf.iter_channels()):            

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
                    "source_uuid": np.full(len(signal.timestamps), str(uuid), dtype=object),
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
                writer = pq.ParquetWriter(targetfile, table.schema, compression="GZIP")

            writer.write_table(table)

        except Exception as e:
            print(f"Signal {counter}: {signal.name} with {len(signal.timestamps)} failed: {e}")    

        end_signal_time = time.time() - start_signal_time
        print(f"Signal {counter}: {signal.name} with {len(signal.timestamps)} entries took {end_signal_time}")

    writer.close()

    return counter

# Writes a gzipped CSV file using the uuid as name
# It will write a file for each individual signal
def writeCsv(basename, mdf, uuid, target):

        # Start time of the recording
        recordingStartTime = mdf.header.start_time

        # Initial counter
        counter = 0

        # Iterate over the signals
        for signals in mdf.iter_channels():            
            
            # open the file in the write mode
            with gzip.open(os.path.join(target, f"{basename}-{uuid}-{counter}.csv.gz"), 'wt') as csvFile:

                writer = csv.writer(csvFile)
                writer.writerow(["source_uuid", "name", "unit", "relativeTimestamp", "absoluteTimestamp", "value", "value_string", "source", "source_type", "bus_type", "timeDifference"])

                try:
                    numericSignals = signals.samples.astype(np.double)
                    stringSignals = np.empty(len(signals.timestamps), dtype=str)
                    importType = "numeric"
                except:
                    numericSignals = np.full(len(signals.timestamps), dtype=np.double, fill_value=0)
                    stringSignals = signals.samples.astype(str)
                    importType = "string"
                
                print(f"Exporting signal: {signals.name} as type {importType} to file {csvFile.name}")


                # Iterate on the entries for the signal
                previousRecordTime = 0

                for indx in range(0, len(signals.timestamps)):

                    currentRecordTime = signals.timestamps[indx]

                    try:
                        numericValue = float(signals.samples[indx])
                    except:
                        numericValue = "",

                    writer.writerow(
                        [
                            str(uuid),
                            signals.name, 
                            signals.unit, 
                            currentRecordTime,
                            recordingStartTime + timedelta(seconds=currentRecordTime),
                            numericSignals[indx],
                            stringSignals[indx],
                            signals.source.name,
                            v4c.SOURCE_TYPE_TO_STRING[signals.source.source_type],
                            v4c.BUS_TYPE_TO_STRING[signals.source.bus_type],
                            currentRecordTime - previousRecordTime
                        ]
                    )

                    previousRecordTime = currentRecordTime

            csvFile.close()

            counter +=  1
            
        return counter

# Process a single file
def processFile(filename):    
    
    start_time = time.time()

    mdf = MDF(filename)
    basename = Path(filename).stem

    file_uuid = uuid.uuid4()
    
    if (args.dump):
        dumpSignals(basename, mdf, file_uuid)

    numberOfChunks = 0

    # Use the right method based on the format
    if (args.exportFormat == "parquet"):         
        numberOfChunks = writeParquet(basename, mdf, file_uuid, args.target)
    else:        
        numberOfChunks = writeCsv(basename, mdf, file_uuid, args.target)        
    
    writeMetadata(basename, mdf, file_uuid, args.target, numberOfChunks)

    end_time = time.time() - start_time
    print (f"Processing {filename} took {end_time}")

# Process a complete directory
def processDirectory(directoryname):
    for path in os.listdir(directoryname):
        if os.path.isfile(os.path.join(directoryname, path)):
            processFile(os.path.join(directoryname, path))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single MDF-4 or directory with MDF-4 files into CSV or Parquet Files")
    parser.add_argument("-f", "--file", dest="file", help="Path to a single MDF-4 file")
    parser.add_argument("-d", "--directory", dest="directory", help="Path to a directory with MDF-4 files")
    parser.add_argument("-t", "--target", dest="target", default=".", help="Location where the processed files will be stored")
    parser.add_argument("--dump", dest="dump", action="store_true", help="Shows the content of the files")
    parser.add_argument("--format", dest="exportFormat", default="csv", help="Use csv or parquet to select your export format")
    args = parser.parse_args()

    if(args.file):
        processFile(args.file)

    if(args.directory):
        processDirectory(args.directory)



