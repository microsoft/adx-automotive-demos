from asammdf import MDF
from asammdf.blocks import v4_constants as v4c
from datetime import datetime, timedelta
import time 
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
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
    print(f"pid {os.getpid()}: Launched task signal {counter}: {signalMetadata['name']}")
    start_signal_time = time.time()

    try:        
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

        try:
            channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, decodedSignal)    
            numericSignals, stringSignals = extractSignalsByType(decodedSignal, rawSignal)
            source_type = v4c.SOURCE_TYPE_TO_STRING[decodedSignal.source.source_type]
            bus_type = v4c.BUS_TYPE_TO_STRING[decodedSignal.source.bus_type]

            table = pa.table (
                {                   
                    "source_uuid": np.full(len(decodedSignal.timestamps), str(uuid), dtype=object),
                    "name": np.full(len(decodedSignal.timestamps), decodedSignal.name, dtype=object),
                    "unit": np.full(len(decodedSignal.timestamps), decodedSignal.unit, dtype=object),
                    "timestamp": decodedSignal.timestamps,
                    "timestamp_diff": np.append(0, np.diff(decodedSignal.timestamps)),
                    "value": numericSignals,
                    "value_string": stringSignals,
                    "valueRaw" : rawSignal.samples,
                    "source": np.full(len(decodedSignal.timestamps), decodedSignal.source.name, dtype=object),
                    "channel_group_acq_name": np.full(len(decodedSignal.timestamps), channel_group_acq_name, dtype=object),
                    "acq_source_name": np.full(len(decodedSignal.timestamps), acq_source_name, dtype=object),
                    "acq_source_path": np.full(len(decodedSignal.timestamps), acq_source_path, dtype=object),
                    "source_type": np.full(len(decodedSignal.timestamps), source_type, dtype=object),
                    "bus_type": np.full(len(decodedSignal.timestamps), bus_type, dtype=object)
                }
            ) 
            

            pq.write_to_dataset(table, root_path=targetdir, use_threads=False, compression="snappy")                
            
        except Exception as e:
            return (f"pid {os.getpid()}", False, counter, f"Signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} failed: {str(e)}")
        
        end_signal_time = time.time() - start_signal_time        

        return (f"pid {os.getpid()}", True, counter, f"Processed signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} entries in {end_signal_time}")       
    
    except Exception as e:
        return (f"pid {os.getpid()}", False, counter, f"Signal {counter}: {decodedSignal.name} with {len(decodedSignal.timestamps)} failed: {str(e)}")
    
    finally:
        mdf.close()
        del decodedSignal, rawSignal, mdf
