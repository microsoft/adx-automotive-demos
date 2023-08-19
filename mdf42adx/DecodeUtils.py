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

def extractSignalsByType(signal):
    '''
        Extracts the signals from the MDF-4 file and converts them to a numeric or string representation
        Takes into consideration numbers, strings and records (rendered as a string) 
    '''   

    if np.issubdtype(signal.samples.dtype, np.record):
        numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)      
        stringSignals = [record.pprint() for record in signal.samples]

    elif np.issubdtype(signal.samples.dtype, np.number):
        numericSignals = signal.samples.astype(np.double)
        stringSignals = np.empty(len(signal.timestamps), dtype=str) 

    else:
        numericSignals = np.full(len(signal.timestamps), dtype=np.double, fill_value=0)            
        stringSignals = signal.samples.astype(str)
    return numericSignals,stringSignals