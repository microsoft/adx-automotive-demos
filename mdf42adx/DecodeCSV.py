from asammdf import MDF
import csv
import gzip
import os
import time 
import re
import multiprocessing as mp
from DecodeUtils import getSource, extractSignalsByType

def processSignalAsCsv(counter, filename, signalMetadata, uuid, targetdir, blacklistedSignals):

    start_signal_time = time.time()
    print(f"pid {os.getpid()}: Launched task signal {counter}: {signalMetadata['name']}")

    if signalMetadata["name"] in blacklistedSignals:
        return (f"pid {os.getpid()}", False, counter, f">>> Skipped: {signalMetadata}")

    # Get the signal group and channel index to load that specific signal ONLY
    group_index = signalMetadata["group_index"]
    channel_index = signalMetadata["channel_index"]

    # Open the MDF file and select a single signal
    mdf = MDF(filename)          
    # We select a specific signal, both decoded and raw
    decodedSignal = mdf.select(channels=[(None, group_index, channel_index)])[0]
    rawSignal = mdf.select(channels=[(None, group_index, channel_index)], raw=True)[0]
    
    print(f"pid {os.getpid()}: Processing signal {counter}: {decodedSignal.name} group index {group_index} channel index {channel_index} with type {decodedSignal.samples.dtype}")   


    # Escape all characters from the decodedSignal.name and use only alphanumeric and underscore for the basename
    # This is to avoid issues with the basename_template and parquet
    escaped_signal_name = re.sub(r"[^a-zA-Z0-9_]", "_", decodedSignal.name)
    targetfile = os.path.join(targetdir, f"{escaped_signal_name}-{uuid}-{counter}.csv.gz")
    os.makedirs(os.path.dirname(targetfile), exist_ok=True)

    # open the file in the write mode
    with gzip.open(targetfile, 'wt') as csvFile:

        floatSignals, integerSignals, uint64Signals, stringSignals = extractSignalsByType(decodedSignal=decodedSignal, rawSignal=rawSignal)                       

        writer = csv.writer(csvFile)
        writer.writerow(["source_uuid", "group_index", "channel_index", "name", "unit", "timestamp", "value", "value_string", "value_raw"])                
                        
        # Iterate on the entries for the signal
        for indx in range(0, len(decodedSignal.timestamps)):

            writer.writerow(
                [
                    str(uuid),
                    group_index,
                    channel_index,
                    decodedSignal.name, 
                    decodedSignal.unit, 
                    decodedSignal.timestamps[indx],
                    floatSignals[indx],
                    stringSignals[indx],
                    rawSignal[indx],
                ]
            )

    csvFile.close()
    
    end_signal_time = time.time() - start_signal_time
    mdf.close()
    
    return (f"Signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} entries took {end_signal_time}", len(decodedSignal.timestamps)) # Last position is here the no. of entries count - length will suffice to check no. of entries 