FROM python:3.9

WORKDIR /app

# Copy the dependent scripts to the /app/ directory within the container
COPY DecodeParquet.py /app/
COPY DecodeUtils.py /app/
COPY DecodeCSV.py /app/ # *** MDF2AnalyticsFormatProcessing has a dependency on this script ***
COPY MetadataTools.py /app/
COPY AzureBatch.py /app/
COPY MDF2AnalyticsFormatProcessing.py /app/
COPY AzBatchMDF2AnalyticsFormat.py /app/

# Set the appropriate permissions for the /app/ directory -  
RUN chmod -R 777 /app/

# Install any additional dependencies if required
RUN pip install setuptools==57.5.0

# Install other libraries
RUN pip install asammdf==7.3.14 pathlib==1.0.1 numpy==1.25.2 uuid==1.30 pyarrow==12.0.1 azure-core==1.29.1 psutil==5.9.6 

CMD ["python", "/app/AzBatchMDF2AnalyticsFormat.py"]
