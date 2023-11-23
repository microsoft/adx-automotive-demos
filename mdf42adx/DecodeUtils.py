# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import numpy as np
from asammdf.blocks import v4_constants as v4c
import traceback

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

    try:
        channel_group_acq_source_comment = mdf.groups[signal.group_index].channel_group.acq_source.comment
    except:
        channel_group_acq_source_comment = ""

    try:
        channel_group_comment = mdf.groups[signal.group_index].channel_group.comment
    except:
        channel_group_comment = ""

    try:
        signal_source_path = signal.source.path
    except:
        signal_source_path = ""

    return source_name, source_type, bus_type, channel_group_acq_name, acq_source_name, acq_source_path, channel_group_acq_source_comment, channel_group_comment, signal_source_path

def extractSignalsByType(decodedSignal, rawSignal):
    '''
        Extracts the signals from the MDF-4 file and converts them to a numeric or string representation
        Takes into consideration numbers, strings and records (rendered as a string) 

        We have to make sure that we have the right type / storage based on the datatype.
        Trying to use the wrong type will create issues related to loss of precision.

        ADX real datatype is a 64 bit float.
        This means that all integer types except uint64 and int64 can be stored without loss of precision        

    '''   
    numberOfSamples = len(decodedSignal.timestamps)

    # create an empty array for each type of signal initialized to nan or zero values
    floatSignals = np.full(numberOfSamples, np.nan, dtype=np.double)
    stringSignals = np.empty(numberOfSamples, dtype=str)

    try:
        # If it is a record we will decompose its contents on the string field
        # we will not store a value in floatSignals
        if np.issubdtype(decodedSignal.samples.dtype, np.record):
            stringSignals = [record.pprint() for record in decodedSignal.samples]

        # If the value can be represented as a float is the only thing we need.
        # String will be empty
        elif np.issubdtype(decodedSignal.samples.dtype, np.floating):
            floatSignals = decodedSignal.samples

        # Check if decodedSignal.samples.dtype is a uint64 or uint. If it is, we will only store it as string
        # Floats will not be stored as there is a loss of precision
        elif np.issubdtype(decodedSignal.samples.dtype, np.uint64) or np.issubdtype(decodedSignal.samples.dtype, np.int64):        
            stringSignals = decodedSignal.samples.astype(str)   
    
        # We will store all ints smaller or equal to 32 bits in floats only, as we have no loss of precision
        elif np.issubdtype(decodedSignal.samples.dtype, np.integer):
            floatSignals = decodedSignal.samples
        
        # If we have a pure string as raw signal, we will store it as a string
        elif np.issubdtype(rawSignal.samples.dtype, np.string_) or np.issubdtype(rawSignal.samples.dtype, np.unicode_):
            stringSignals = rawSignal.samples.view(np.chararray).decode('utf-8') 

        # For everything else use the previous approach but we will use decode with utf-8 to make sure we get the correct representation for text tables
        # astype(string) was causing issues with special characters, and S32 would have truncated results.
        else:
            floatSignals = rawSignal.samples.astype(float)
            stringSignals = decodedSignal.samples.view(np.chararray).decode('utf-8') 
            

    except Exception as e:
        print(f"Exception for {decodedSignal.name}: {e}")        
        print(traceback.print_exc())        
        raise e

    return floatSignals, stringSignals