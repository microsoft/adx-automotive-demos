# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import argparse
import numpy as np
import pandas as pd
from asammdf import MDF
import csv
import gzip
import uuid
from datetime import datetime, timedelta


# Iterates over all signals and prints them to the console
def dumpSignals(mdf):
    for signals in mdf.iter_channels():

        try:
            numericSignals = signals.samples.astype(np.double)
        except:
            try: 
                numericSignals = signals.samples.astype(np.int64)            
            except:
                numericSignals = np.empty(len(signals.timestamps))    

        for indx in range(0, len(signals.timestamps)):                               

           print(
                [
                    args.file,
                    uuid,
                    signals.name, 
                    signals.unit, 
                    signals.timestamps[indx],
                    numericSignals[indx],
                    signals.source.source_type,
                    signals.source.bus_type
                ]
            )


# Creates a metadata file for the MDF-4
def writeMetadata(mdf, uuid):
    metadataFile = open(str(uuid) + ".md", 'w')
    metadataFile.write(args.file)
    metadataFile.write(mdf.header.comment)
    metadataFile.close()

# Writes a gzipped CSV file using the uuid as name
def writeCsv(mdf, uuid):
    # open the file in the write mode
    with gzip.open(str(uuid) + "-signalscsv.gz", 'wt') as csvFile:

        print("Exporting to: " + csvFile.name)

        writer = csv.writer(csvFile)
        writer.writerow(["source_file","source_uuid", "name", "unit", "relativeTimestamp", "absoluteTimestamp", "value", "value_string", "source_type", "bus_type"])

        # Start time of the recording
        recordingStartTime = mdf.header.start_time

        for signals in mdf.iter_channels():

            try:
                numericSignals = signals.samples.astype(np.double)
            except:
                try: 
                    numericSignals = signals.samples.astype(np.int64)            
                except:
                    numericSignals = np.empty(len(signals.timestamps))    

            print("Exporting signal: ", signals.name)               

            for indx in range(0, len(signals.timestamps)):

                try:
                    numericValue = float(signals.samples[indx])
                except:
                    numericValue = "",

                writer.writerow(
                    [
                        args.file,
                        uuid,
                        signals.name, 
                        signals.unit, 
                        signals.timestamps[indx],
                        recordingStartTime + timedelta(seconds=signals.timestamps[indx]),
                        numericSignals[indx],
                        signals.samples[indx],
                        signals.source.source_type,
                        signals.source.bus_type
                    ]
                )

    csvFile.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    
        
    args = parser.parse_args()

    mdf = MDF(args.file)
    uuid = uuid.uuid4()

    
    #dumpSignals(mdf)
    
    writeMetadata(mdf, uuid)
    writeCsv(mdf, uuid)
    


