# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from AzureEventGrid import AzureEventGridSendEvent, AzureEventGridSendEventInsert, AzureEventGridSendEventUpdates



def AzureBatchEnvironmentVariables():
    '''
        Azure Batch provides Batch specific environment variables. Additionally, custom environment variables can be passed during the Azure Batch Pool/Task configutations.
        Below are the important env variables to consider:

            AZ_BATCH_TASK_WORKING_DIR -> Can be used to locate the resource file (input file) as part of decoding process
            CUSTOM_FILE_NAME -> The file name including the extension, e.g. AAAA.mf4
    '''

    # Azure Batch Specific configuration that we are retrieving to locate the file when resource file loads in the file on VM volume
    taskWorkingDirectory = os.environ.get('AZ_BATCH_TASK_WORKING_DIR') # The working directory of the VM that is provisioned 
    taskBatchDirectory = os.environ.get('AZ_BATCH_TASK_DIR') # Task working directory where task is located *for decoder productionisation not needed*
    taskNodeRootDirectory = os.environ.get('AZ_BATCH_NODE_ROOT_DIR')  # The Node directory for all tasks - essentially for the Batch job *for decoder productionisation not needed*
    fileNameEnvVar = os.environ.get('CUSTOM_FILE_NAME') # Custom environment variable that is needed to point to the mf4 file (e.g. xxxxx.mf4)
    print(f"Current working directory: {taskWorkingDirectory}")

    return taskWorkingDirectory, taskBatchDirectory, taskNodeRootDirectory, fileNameEnvVar





def AzureBatchProcessFilesOutputFolder(fileNameEnvVar):
    '''
       To save the decoded files back into a separate directory use this function. In this case, we are creating an 'output' folder to save all decoded files in.
       Note that when running this a docker container, you may explicity need to give 'RUN chmod -R 777' in the Dockerfile blueprint.
    '''
    try:   
        outputFolder = f"{os.environ.get('AZ_BATCH_TASK_WORKING_DIR')}/output/"
        if(os.path.exists(outputFolder)):
            print("Output Path Exists")
        else:
            os.makedirs(outputFolder)
            print("Created output folder")

        return outputFolder

    except OSError as oserr:
        print(f"Error occurred when locating/creating output folder for file {fileNameEnvVar}.\n{oserr}")
        raise

    except Exception as generalErr:
        print(f"Error occured when creating/locating Output folder for file {fileNameEnvVar}.\n{generalErr}")
        raise