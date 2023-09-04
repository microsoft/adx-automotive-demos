import numpy as np
from asammdf.blocks import v4_constants as v4c

def getSource(mdf, signal):    
    '''
        Extracts the source information from the MDF-4 file for a given signal
    '''

    if signal.source is not None:
        source_name = signal.source.name
        source_type = v4c.SOURCE_TYPE_TO_STRING[signal.source.source_type]
        bus_type = v4c.BUS_TYPE_TO_STRING[signal.source.bus_type]
    else:
        source_name = "Unknown"
        source_type = "Unknown"
        bus_type = "Unknown"

    try: 
        channel_group_acq_name = mdf.groups[signal.group_index].channel_group.acq_name
    except:
        channel_group_acq_name = ""

    try: 
        acq_source_name = mdf.groups[signal.group_index].channel_group.acq_source.name
    except:
        acq_source_name = ""

    try:
        acq_source_path = mdf.groups[signal.group_index].channel_group.acq_source.path
    except:
        acq_source_path = ""

    return source_name, source_type, bus_type, channel_group_acq_name, acq_source_name, acq_source_path

def extractSignalsByType(decodedSignal, rawSignal):
    '''
        Extracts the signals from the MDF-4 file and converts them to a numeric or string representation
        Takes into consideration numbers, strings and records (rendered as a string) 
    '''   
    	
    if np.issubdtype(decodedSignal.samples.dtype, np.record):
        numericSignals = rawSignal.samples
        stringSignals = [record.pprint() for record in decodedSignal.samples]

    elif np.issubdtype(decodedSignal.samples.dtype, np.number):
        numericSignals = decodedSignal.samples.astype(np.double)
        stringSignals = np.empty(len(decodedSignal.timestamps), dtype=str) 

    else:
        numericSignals = rawSignal.samples
        stringSignals = decodedSignal.samples.astype(str)

    return numericSignals,stringSignals