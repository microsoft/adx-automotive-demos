# Prepare an MDF-4 file for import in ADX

The [ASAM MDF-4 standard](https://www.asam.net/standards/detail/mdf/wiki/) has wide adoption in the automotive industry to store measurement and calibration data. The [asammdf python library](https://pypi.org/project/asammdf/) provides structured access to the MDF-4 data.

The PrepareMDF4FileForADX will take a MDF-4 file as argument and create a CSV file that can be directly ingested into ADX.

# Usage
- Create a python virtual environment using the provided requirements.txt
- Execute the commmand using

``` bash
python PrepareMDF4FileForADX.py <file.mdf>
```

The script will create two files using a new UUID 
- A gzipped CSV file that can be used with the ADX "Ingest Data" function
- A file containing extracted metadata