from asammdf import MDF
from asammdf.blocks import v4_constants as v4c
import csv
import gzip
import os
import time 
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
    signals = mdf.select([(None, group_index, channel_index)])        
    
    print(f"pid {os.getpid()}: MDF file open signal {counter}: {signalMetadata['name']} - {filename} opened group {group_index}, channel {channel_index})")
    
    for signal in signals:
        print(f"pid {os.getpid()}: Processing signal {counter}: {signal.name} group index {group_index} channel index {channel_index} with type {signal.samples.dtype}")   

        # open the file in the write mode
        with gzip.open(os.path.join(targetdir, f"{uuid}-{counter}.csv.gz"), 'wt') as csvFile:

            channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, signal)    
            numericSignals, stringSignals = extractSignalsByType(signal)

            writer = csv.writer(csvFile)
            writer.writerow(["source_uuid", "name", "unit", "timestamp", "value", "value_string", "source", "channel_group_acq_name", "acq_source_name", "acq_source_path", "source_type", "bus_type"])                
                            
            # Iterate on the entries for the signal
            for indx in range(0, len(signal.timestamps)):

                writer.writerow(
                    [
                        str(uuid),
                        signal.name, 
                        signal.unit, 
                        signal.timestamps[indx],
                        numericSignals[indx],
                        stringSignals[indx],
                        signal.source.name,
                        channel_group_acq_name,
                        acq_source_name,
                        acq_source_path,                    
                        v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type],
                        v4c.BUS_TYPE_TO_STRING[signal.source.bus_type],
                    ]
                )

        csvFile.close()
    
    end_signal_time = time.time() - start_signal_time
    print(f"Signal {counter}: {signal.name} with {len(signal.timestamps)} entries took {end_signal_time}")