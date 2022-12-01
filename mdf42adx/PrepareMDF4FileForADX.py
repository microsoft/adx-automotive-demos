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

# Iterates over all signals and prints them to the console
def dumpSignals(basename, mdf, uuid):
       
    for signals in mdf.iter_channels():

        try:
            numericSignals = signals.samples.astype(np.double)
        except:     
            numericSignals = np.empty(len(signals.timestamps))   

        for indx in range(0, len(signals.timestamps)):                               

           print(
                [
                    str(uuid),
                    signals.name, 
                    signals.unit, 
                    signals.timestamps[indx],
                    numericSignals[indx],
                    signals.source.source_type,
                    signals.source.bus_type
                ]
            )

# Creates a metadata file for the MDF-4
def writeMetadata(basename, mdf, uuid, target):
    with open(os.path.join(target, f"{basename}-{uuid}.metadata.json"), 'w') as metadataFile:
        metadata = {
            "name": basename,
            "uuid": str(uuid),
            "comments": mdf.header.comment
        }
        metadataFile.write(json.dumps(metadata))

# Create a parquet file for the MDF
def writeParquet(basename, mdf, uuid, target):    
    targetfile = os.path.join(target, f"{basename}-{uuid}-signals.parquet")
    print(f"Exporting Parquet to: {targetfile}")
    mdf.export(fmt="parquet", filename=targetfile, raw=False, empty_channels="skip", ignore_value2text_conversions = False, time_from_zero=False, compression="GZIP")

# Writes a gzipped CSV file using the uuid as name
def writeCsv(basename, mdf, uuid, target):
    # open the file in the write mode
    with gzip.open(os.path.join(target, f"{basename}-{uuid}-signalscsv.gz"), 'wt') as csvFile:

        print(f"Exporting CSV to: {csvFile.name}")

        writer = csv.writer(csvFile)
        writer.writerow(["source_uuid", "name", "unit", "relativeTimestamp", "absoluteTimestamp", "value", "value_string", "source_type", "bus_type"])

        # Start time of the recording
        recordingStartTime = mdf.header.start_time

        for signals in mdf.iter_channels():            

            try:
                numericSignals = signals.samples.astype(np.double)
                stringSignals = np.empty(len(signals.timestamps), dtype=str)
                importType = "numeric"
            except:
                numericSignals = np.full(len(signals.timestamps), dtype=np.double, fill_value=0)
                stringSignals = signals.samples.astype(str)
                importType = "string"

            print(f"Exporting signal: {signals.name} as type {importType}")               

            for indx in range(0, len(signals.timestamps)):

                try:
                    numericValue = float(signals.samples[indx])
                except:
                    numericValue = "",

                writer.writerow(
                    [
                        str(uuid),
                        signals.name, 
                        signals.unit, 
                        signals.timestamps[indx],
                        recordingStartTime + timedelta(seconds=signals.timestamps[indx]),
                        numericSignals[indx],
                        stringSignals[indx],
                        signals.source.source_type,
                        signals.source.bus_type
                    ]
                )

    csvFile.close()

# Process a single file
def processFile(filename):    
    
    start_time = time.time()

    mdf = MDF(filename)
    basename = Path(filename).stem

    file_uuid = uuid.uuid4()
    
    if (args.dump):
        dumpSignals(basename, mdf, file_uuid)

    if (args.exportFormat == "parquet"):         
        writeMetadata(basename, mdf, file_uuid, args.target)
        writeParquet(basename, mdf, file_uuid, args.target)
    else:
        writeMetadata(basename, mdf, file_uuid, args.target)
        writeCsv(basename, mdf, file_uuid, args.target)        

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



