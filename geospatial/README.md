# Fleet Analytics Dashboard - Understand the data that your vehicles generate

Visualize the aggregated data from your vehicle fleet in beautiful 3D maps.

This NodeJS application uses Azure Data Explorer and Azure Maps to create 3D renderings of vehicle data.
We include several visualization types to get you started:

- Heatmap: Show where the vehicles commonly report location
- Safety first: Visualization of average speed and harsh driving events.
- Signal strength: cell coverage and infrastructure.

The demo queries an Azure Data Explorer cluster using the geospatial extensions to cluster the vehicle data and generate a GeoJson file. The file is loaded using a Data Source into Azure Maps and rendered with polygon extrusion layers.

This application is based on the architecture described in the AAC article [Data Analytics for Test and Validation Fleets](https://learn.microsoft.com/en-us/azure/architecture/industries/automotive/automotive-telemetry-analytics).

## Use the application

Perform the following steps to use the application:
- If you don't have an Azure subscription, create a [free account](https://azure.microsoft.com/free) before you begin.
- Create a [Kusto Cluster](https://learn.microsoft.com/en-us/azure/data-explorer/start-for-free-web-ui)
- Create a table in Kusto with the following structure

```Kusto
    .create table carTelemetry (vin:string,device:string,error:string,dataRecordingTime:datetime,eventType:string,eventValue:dynamic,latitude:real,longitude:real,h3Big:string,h3Medium:string,h3Small:string,telemetryType:string,signal:string,signalValueString:string,signalValueDouble:real)
```

- CREATE App Registration in Azure AD to connect to Azure Data Explorer (Kusto).
    - Give API Permissions (READ) to Azure Data Explorer Service.
    - Create a secret. Copy the value of the secretID (this is your appkey parameter). Suggestion: Move the Application Settings parameters into a KeyVault.
    - In the authentication tab, in the URI Redirect you can use any http endpoint. Use <http://localhost:3000/> for running locally (development)
    - In the authentication tab, in the Implicit grant and hybrid flows, Check the option ID Tokens.
    - ADD AAD App Principal Permissions to the Kusto Cluster.
```Kusto
.add database raventelemetry viewers ('aadapp=<your_app_id>;<your_tenant_id>') 'Dashboard'
```
- Create an [Azure Maps account](https://learn.microsoft.com/en-us/azure/azure-maps/quick-demo-map-app)
- Create a copy of the sample.env file to .env file and write the necessary secrets for application registration and Azure Maps

## Application structure

The following files contain the main logic:

- The **app.js** file describes the basic shape of the application and dependencies
- The **kusto.js** file contains the code that queries the ADX Cluster
- The **fleet-geospatial.js** contains the code that creates the map and renders the data sources

The following libraries and samples played a big role:
- [Bootstrap samples dashboard](https://getbootstrap.com/docs/4.0/examples/dashboard/) to create the main user interface.
- The [Gridded Data Source](https://learn.microsoft.com/en-us/samples/azure-samples/azure-maps-gridded-data-source/azure-maps-gridded-data-source-module/) example from Azure Maps