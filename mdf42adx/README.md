# Using Azure Data Explorer to analyze MDF files

This demo project illustrates the usage of Azure Data Explorer to ingest and analyze MDF-4 Data.

The [ASAM MDF-4 standard](https://www.asam.net/standards/detail/mdf/wiki/) has wide adoption in the automotive industry to store measurement and calibration data. The [asammdf python library](https://pypi.org/project/asammdf/) provides structured access to the MDF-4 data.

The project folder contains an ingestion preparation script, a sample data structure for ADX, sample functions to process the data and a dashboard.

## Step Overview

* Create a [free Azure Data Explorer Cluster](https://learn.microsoft.com/azure/data-explorer/start-for-free-web-ui) and create a database.
* Execute the deployment.kql scripts to create tables and script.
* Prepare a development environment to run the MDF preparation script.
* Process some MDF-4 files.
* Ingest the files in ADX using the [ingest data wizard](https://learn.microsoft.com/azure/data-explorer/ingest-data-wizard) option in ADX.
* Run the prepared queries to get familiar with the data.
* Import the dashboard to see visualizations in ADX.

### Ingestion Preparation Script

The PrepareMDF4FileForADX will take a MDF-4 file as argument and create parquet files that can be directly ingested into ADX.

- Create a python virtual environment using the provided requirements.txt
- Execute the following command to see available options

``` bash
python PrepareMDF4FileForADX.py --help
```

The script will create several files
- A set of parquet or CSV files
- A JSON metadata file containing the information about the MDF-4 file.

