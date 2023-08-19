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

    start_signal_time = time.time()
    print(f"pid {os.getpid()}: Launched task signal {counter}: {signalMetadata['name']}")

    try:        

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
            
            try:
                channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, signal)    
                numericSignals, stringSignals = extractSignalsByType(signal)
                
                table = pa.table (
                    {                   
                        "source_uuid": np.full(len(signal.timestamps), str(uuid), dtype=object),
                        "name": np.full(len(signal.timestamps), signal.name, dtype=object),
                        "unit": np.full(len(signal.timestamps), signal.unit, dtype=object),
                        "timestamp": signal.timestamps,
                        "timestamp_diff": np.append(0, np.diff(signal.timestamps)),
                        "value": numericSignals,
                        "value_string": stringSignals,
                        "source": np.full(len(signal.timestamps), signal.source.name, dtype=object),
                        "channel_group_acq_name": np.full(len(signal.timestamps), channel_group_acq_name, dtype=object),
                        "acq_source_name": np.full(len(signal.timestamps), acq_source_name, dtype=object),
                        "acq_source_path": np.full(len(signal.timestamps), acq_source_path, dtype=object),
                        "source_type": np.full(len(signal.timestamps), v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type], dtype=object),
                        "bus_type": np.full(len(signal.timestamps), v4c.BUS_TYPE_TO_STRING[signal.source.bus_type], dtype=object)
                    }
                ) 
                pq.write_to_dataset(table, root_path=targetdir, compression="snappy")                
                
            except Exception as e:
                return (f"pid {os.getpid()}", False, counter, f"Signal {counter}: {signal.name} with {len(signal.timestamps)} failed: {str(e)}")

            
            end_signal_time = time.time() - start_signal_time        

        return (f"pid {os.getpid()}", True, counter, f"Processed signal {counter}: {signal.name} with {len(signal.timestamps)} entries in {end_signal_time}")       
    
    except Exception as e:
        return (f"pid {os.getpid()}", False, counter, f"Signal {counter}: {signal.name} with {len(signal.timestamps)} failed: {str(e)}")
    
    finally:
        print(f"pid {os.getpid()}: Closed signal {counter}: {signalMetadata['name']} MDF file {filename} closed")
        mdf.close()
        del signals
        del mdf
        print(f"pid {os.getpid()}: Finished task signal {counter}: {signalMetadata['name']}")
