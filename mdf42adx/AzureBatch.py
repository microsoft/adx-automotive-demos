# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os


'''
    Below function will allow Azure Batch Tasks to locate the resource file (input file)
'''
def AzureBatchEnvironmentVariables():
    # Azure Batch Specific configuration that we are retrieving to locate the file when resource file loads in the file on VM volume
    taskWorkingDirectory = os.environ.get('AZ_BATCH_TASK_WORKING_DIR') # The working directory of the VM that is provisioned 
    taskBatchDirectory = os.environ.get('AZ_BATCH_TASK_DIR') # Task working directory where task is located *for decoder productionisation not needed*
    taskNodeRootDirectory = os.environ.get('AZ_BATCH_NODE_ROOT_DIR')  # The Node directory for all tasks - essentially for the Batch job *for decoder productionisation not needed*
    fileNameEnvVar = os.environ.get('CUSTOM_FILE_NAME') # Custom environment variable that is needed to point to the mf4 file (e.g. xxxxx.mf4)
    print(f"Current working directory: {taskWorkingDirectory}")

    return taskWorkingDirectory, taskBatchDirectory, taskNodeRootDirectory, fileNameEnvVar




'''
    Below function will create a folder to save the decoded files in. Will require 'RUN chmod -R 777' elevated priviledge command to create a folder on Docker linux
'''
def AzureBatchProcessFilesOutputFolder(fileNameEnvVar):
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