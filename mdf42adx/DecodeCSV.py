from asammdf import MDF
from asammdf.blocks import v4_constants as v4c
import csv
import gzip
import os
import time 
import multiprocessing as mp
from DecodeUtils import getSource, extractSignalsByType

def processSignalAsCSV(counter, channel_group_acq_name, acq_source_name, acq_source_path, signal, uuid, targetdir, basename):
    start_signal_time = time.time()
    print(f"Processing signal {counter}: {signal.name} with type {signal.samples.dtype}")   
    numericSignals, stringSignals = extractSignalsByType(signal)

    os.makedirs(targetdir, exist_ok=True)

    # open the file in the write mode
    with gzip.open(os.path.join(targetdir, f"{uuid}-{counter}.csv.gz"), 'wt') as csvFile:

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