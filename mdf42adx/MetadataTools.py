# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from asammdf import MDF
from datetime import datetime
import json
import os
from DecodeUtils import getSource

def dumpSignals(filename):
    '''
        Iterates over all signals and prints them to the console. Used for debugging purposes.

        Args:
            filename: the MDF-4 file to process
    '''
    mdf = MDF(filename)

    for counter, signal in enumerate(mdf.iter_channels(raw=True)):        
        source_name, source_type, bus_type, channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, signal)
        print(f"Gr_I: {signal.group_index}, CH_I: {signal.channel_index}, {signal.name}, unit: {signal.unit}, dt: {signal.samples.dtype}, src: {source_name}, gr_name: {channel_group_acq_name}, {acq_source_name}, path: {acq_source_path}, type: {source_type}, bus: {bus_type}")

    mdf.close()
    del mdf

    return counter

def writeMetadata(filename, basename, uuid, target):
    '''
        Creates an object containing the signals description for the MDF-4 file and writes it as a JSON file.
        It returns an array with the signal description for further processing.        

        Args:
            filename: the MDF-4 file to process
            basename: the base name of the metadata file
            uuid: the UUID that identifies this decoding run
            target: the target directory where to write the metadata file
        Returns:
            the number of signals in the file
    '''

    mdf = MDF(filename)

    with open(os.path.join(target, f"{basename}-{uuid}.metadata.json"), 'w') as metadataFile:
        
        print(f"Writing metadata file {basename}-{uuid}")

        metadata = {
            "name": basename,
            "source_uuid": str(uuid),
            "preparation_startDate": str(datetime.utcnow()),
            "signals": [],
            "signals_comment": [],
            "signals_decoding": [],
            "comments": mdf.header.comment,
        }

        for signal in mdf.iter_channels(raw=True):

            source_name, source_type, bus_type, channel_group_acq_name, acq_source_name, acq_source_path = getSource(mdf, signal)

            metadata["signals"].append(
                {
                    "name": signal.name,
                    "unit": signal.unit,
                    "group_index": signal.group_index,
                    "channel_index": signal.channel_index,
                    "channel_group_acq_name": channel_group_acq_name,
                    "acq_source_name": acq_source_name,
                    "acq_source_path": acq_source_path,
                    "source" : source_name,
                    "source_type": source_type,
                    "bus_type": bus_type,
                    "datatype": signal.samples.dtype.name
                }          
            )

            metadata["signals_comment"].append(signal.comment)
            
            metadata["signals_decoding"].append(str(signal.conversion))

        metadataFile.write(json.dumps(metadata))
       
    print(f"Finished writing metadata file {basename}-{uuid} with {len(metadata['signals'])} signals")

    mdf.close()

    del mdf

    return metadata["signals"]