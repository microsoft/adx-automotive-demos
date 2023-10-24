# Using Azure Data Explorer to analyze MDF files

This demo project illustrates the usage of Azure Data Explorer to ingest and analyze MDF Data.

The [ASAM MDF-4 standard](https://www.asam.net/standards/detail/mdf/wiki/) has wide adoption in the automotive industry to store measurement and calibration data. The [asammdf python library](https://pypi.org/project/asammdf/) provides structured access to the MDF-4 data.

The project folder contains an ingestion preparation script, a sample data structure for ADX, sample functions to process the data and a dashboard.

## Step Overview

* Create a [free Azure Data Explorer Cluster](https://learn.microsoft.com/azure/data-explorer/start-for-free-web-ui) and create a database.
* Execute the deployment scripts to create tables and script.
* Prepare a development environment to run the MDF preparation scripts.
  * (Optional) Generate an MDF-4 file.
  * Process MDF files for ingestion.
* Ingest the files in ADX using the [ingest data wizard](https://learn.microsoft.com/azure/data-explorer/ingest-data-wizard) functionality.
* Run queries on the data.

### Execute the deployment script

The deployment script is stored in deployment.kql. Load the file in Azure Data Explorer using the "File/Open" function and execute all commands.

The script creates two tables, *signals* and *signals_metadata*.

* The *signals* table stores all values contained in the MDF file
* The *signals_metadata* table stores information about the files, such as included signals and group names.

The script also creates mapping for ingestion.

### Create a sample MDF file

If you have no MDF files at hand, use the CreateSampleMDF.py script to generate a basic MDF4 file that contains simulated values
for Engine RPM, Vehicle Speed and Engine Power.

``` bash
python CreateSampleMDF.py --file samplefile.mdf
```

### Process MDF Files for ingestion

The PrepareMDF4FileForADX will take a MDF-4 file as argument and create parquet files that can be directly ingested into ADX.

- Create a python virtual environment using the provided requirements.txt
- Execute the following command to see available options

``` bash
python MDF2AnalyticsFormat.py --help
```

Using the sample file, the command looks like this:

``` bash
python MDF2AnalyticsFormat.py --file samplefile.mf4 --target ~/<mydestinationdir> --format parquet
```

The script will create several files:

* A set of parquet or CSV files, organized by signals.
* A JSON metadata file containing the information about the MDF-4 file.

### Ingest files into ADX

Use the [ingest data wizard](https://learn.microsoft.com/azure/data-explorer/ingest-data-wizard) functionality to ingest the processed files. The ingestion has two steps:
- Ingest the parquet / csv files into the *signals* table.
- Ingest the JSON file into the *signals_metadata* table.

When ingesting, use the pre-created mappings.

### Run demo queries
Open the demo.kql file in Azure Data Explorer using *File/Open*. Follow the instructions to execute some sample queries and visualization on the MDF data.

Some sample queries assume specific signal names - you can adjust the queries to use your own data.
