# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from asammdf import MDF
from datetime import datetime, timedelta
import time 
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import re
from DecodeUtils import getSource, extractSignalsByType

def processSignalAsParquet(counter, filename, signalMetadata, uuid, targetdir, blacklistedSignals):
    '''
        Creates a parquet export with the structure that we will import into ADX.
        There are three important pieces of information for time analysis of automotive signals
            - The signal itself, which might be a record, numeric or string
            - The frame, which might contain multiple signals
            - The bus (some signals might be avaialble on multiple buses such as CAN and LIN)

        Additional information that is relevant for analytis:
            - The source ECU, as some analysis are related to specific Electronic Control Units.
            - The type of BUS, as some analysis are specific to CAN, LIN or ETH.

    '''

    # Get the signal group and channel index to load that specific signal ONLY
    signal_name = signalMetadata["name"]
    group_index = signalMetadata["group_index"]
    channel_index = signalMetadata["channel_index"]
    print(f"pid {os.getpid()}: Processing signal {counter}: {signal_name} group index {group_index} channel index {channel_index}") 


    try:        
        # Open the MDF file and select a single signal
        mdf = MDF(filename)     

        start_signal_time = time.time()

        # If the signal is blacklisted, we skip it and return 0 samples
        if signal_name in blacklistedSignals:
            return (f"pid {os.getpid()}", True, counter, f"Skipped: {signalMetadata}", 0)

        # We select a specific signal, both decoded and raw
        decodedSignal = mdf.select(channels=[(None, group_index, channel_index)])[0]
        rawSignal = mdf.select(channels=[(None, group_index, channel_index)], raw=True)[0]
    

        try:

            numberOfSamples = len(decodedSignal.timestamps)

            # If there are no samples, we report a success but with 0 samples
            if (numberOfSamples == 0):                
                return (f"pid {os.getpid()}", True, counter, f"Processed signal {counter}: {decodedSignal.name} - no samples in file", numberOfSamples)


            floatSignals, integerSignals, uint64Signals, stringSignals = extractSignalsByType(decodedSignal=decodedSignal, rawSignal=rawSignal)                       

            table = pa.table (
                {                   
                    "source_uuid": np.full(numberOfSamples, str(uuid), dtype=object),
                    "group_index": np.full(numberOfSamples, group_index, dtype=np.int32),
                    "channel_index": np.full(numberOfSamples, channel_index, dtype=np.int32),
                    "name": np.full(numberOfSamples, decodedSignal.name, dtype=object),
                    "unit": np.full(numberOfSamples, decodedSignal.unit, dtype=object),
                    "timestamp": decodedSignal.timestamps,
                    "timestamp_diff": np.append(0, np.diff(decodedSignal.timestamps)),
                    "value": floatSignals,
                    "value_int": integerSignals,
                    "value_uint64": uint64Signals,
                    "value_string": stringSignals,
                    "valueRaw" : rawSignal.samples,
                }
            )             

            # Escape all characters from the decodedSignal.name and use only alphanumeric and underscore for the basename
            # This is to avoid issues with the basename_template and parquet
            parquetFileName = re.sub(r"[^a-zA-Z0-9_]", "_", decodedSignal.name)

            pq.write_to_dataset(
                table, 
                root_path=targetdir,
                basename_template=f"{group_index}-{channel_index}-{parquetFileName}-{{i}}.parquet",
                use_threads=True,
                compression="snappy")                
            
        except Exception as e:
            return (f"pid {os.getpid()}", False, counter, f"Signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} failed: {str(e)}", 0)
        
        end_signal_time = time.time() - start_signal_time        
        
        return (f"pid {os.getpid()}", True, counter, f"Processed signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} entries in {end_signal_time}", numberOfSamples)
    
    except Exception as e:
        return (f"pid {os.getpid()}", False, counter, f"Signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} failed: {str(e)}", 0)
    
    finally:
        mdf.close()
        del decodedSignal, rawSignal, mdf
