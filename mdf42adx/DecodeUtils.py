import numpy as np

def getSource(mdf, signal):
    '''
        Extracts the source information from the MDF-4 file for a given signal
    '''

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

    return channel_group_acq_name, acq_source_name, acq_source_path

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