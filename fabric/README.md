# Using Microsoft Fabric to analyze MDF files

This document will walk you through the necessary steps to analyze MDF files in Fabric. We will generate sample MDF files using the *asammdf* library, generate partitioned parquet files and load them in an Eventhouse for analytics with some sample queries. We will also display the resulting information in a Real Time Dashboard.

## Step Overview

- Get a free Fabric trial and create a workspace
- Create the necessary resources
  - Create an Environment that supports asammdf to generate MDF Files
  - Create a Lakehouse to host the parquet files
  - Create an Eventhouse to perform Real Time Analytics
- Setup your Eventhouse using the Vehicle Data - Setup KQL Queryset

![Sample Fabric Configuration](sample-fabric-configuration.svg)

1. Generate synthetic MDF and metadata files.
1. Decode the files into parquet.
1. Map an external table to load the parquet files in Event House.
1. Set up a pipeline to ingest the metadata files.
1. Visualize the results.

## Initial Setup

- Get a [Microsoft Fabric trial capacity](https://learn.microsoft.com/fabric/get-started/fabric-trial) to get started with Fabric
- [Create a workspace in Fabric](https://learn.microsoft.com/fabric/get-started/create-workspaces) and use a suitable name, for example *Automotive Engineering Data*

## Create the necessary resources

We will create a spark environment that has the necessary automotive libraries, a lakehouse to store raw MDF recordings and a eventhouse to store metadata and signal data.

- Create a new environment item called *AutomotiveEnvironment* and add the *asammdf* library from PyPI
  - In the environment, go to the entry "Public Libraries" and press "+ Add from PyPI" on the top bar.
  - Type asammdf and select the library
  - Select the "publish" option
  - Wait until the publication process is complete. This might take a few minutes.
- [Create a Lakehouse](https://learn.microsoft.com/fabric/data-engineering/create-lakehouse) and call it *LH_VehicleData*. The Lakehouse will store the MDF and the parquet files.
- [Create a new Eventhouse](https://learn.microsoft.com/fabric/real-time-intelligence/create-eventhouse) item and call it *EvH_VehicleData*. The Eventhouse will have a KQL Database to store the vehicle data.

## Generate sample MDF files and process them

- Create a new notebook and copy the content of the [Vehicle Data - File Creation](VehicleData-FileCreation.ipynb) notebook code.
  - Assign the *LH_VehicleData* lakehouse to the notebook,
  - Select the previously created *AutomotiveEnvironment* as the execution environment from the toolbar
- Execute the cell *Create a Sample MDF File* to create a sample MDF file. This file will be stored in the Lakehouse under the Files/sample path.
- Execute the cell *Create metadata file*. This will create the associated metadata file to the recording in the Files/metadata path.
- Execute the cell *Decode the MDF file to parquet*. This will transform all MDF files in the Files/Sample into a format suitable for analytics (parquet), partitioned by source_uuid. The source_uuid is generated automatically for each generated MDF file. The result is stored the File/raw path.

## Setup your eventhouse

- Create a KQL Query Set item with the name "Vehicle Data - Setup" and add the code from the [KQL Setup file](VehicleData-Setup.kql)
  - Assign the database *EvH_VehicleData* to the notebook.
  - Change the location to point to your lakehouse. You can find the URL in the properties of the lakehouse folder.
  - Execute the query to create the *raw* external table.
  - Execute the query to create the *metadata* table
- Create a pipeline *load-metadata* to automatically import the metadata files into the metadata table.
  - Select the copy data assistant
  - Select *OneLake data hub* on the top, then select the lakehouse *LH_VehicleData*, the option *Files* and then select the folder *metadata*, and expand to show the contents of the folder.
  - Select File format as JSON and wait for the data preview to load. Verify that the JSON file contains the information aobut the MDF file.
  - Select the *EvH_VehicleData eventhouse* and the table *metadata* as destinations
  - Check the mappings (note: you can delete the unassigned entries for this demo)
  - Save and run the import. It takes a few minutes to complete.

## Execute some sample queries

- Create a KQL Query set item with the name *Vehicle Data - Sample Queries* 
  - Assign the EvH_Vehicle data
  - Add the code from [Vehicle Data - Sample Queries](VehicleData-SampleQueries.kql)
  - Read the description and execute the queries to visualize the imported files and the signal data.

## Visualize the data a Real Time Dashboard

- Create a new Real-Time Dashboard with the name *EngineeringVisualization*
- In manage, select *Replace with file* and import the [Real Time Dashboard](Dashboard-Engineering.json)
- Click on *DataSources* and click on the "edit" button of the EvH_VehicleData source. Select the right database and click apply.

## Clean up

- Delete the workspace from Microsoft Fabric to clear all resources.
