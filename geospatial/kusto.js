// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
require('dotenv').config();
const KustoClient = require("azure-kusto-data").Client;
const KustoConnectionStringBuilder = require("azure-kusto-data").KustoConnectionStringBuilder;
const ClientRequestProperties = require("azure-kusto-data").ClientRequestProperties;
const { v4: uuidv4 } = require('uuid');


const clusterConnectionString = process.env.CLUSTER_CONNECTION_STRING;
const aadAppId = process.env.AAD_APP_ID; 
const appKey = process.env.AAD_APP_KEY;
const tenantID = process.env.AAD_TENANT_ID; 
const database = process.env.DATABASE_NAME;

// -- the appid parameter is the Application (client) ID guid from the overview page of the app registration
// -- the tenantID parameter is the Directory (tenant) ID guid from the overview page of the app registration
const kcs = KustoConnectionStringBuilder.withAadApplicationKeyAuthentication(clusterConnectionString, aadAppId, appKey, tenantID);

const kustoClient = new KustoClient(kcs);

// The queries assume that specific signals are available.
// Modify the signals to match your data


// Coverage of cell phone strength
// This query will use the "gsmSignal" and group it by the h3small cell.
module.exports.queryCellPhoneCoverage = async function queryCellPhoneCoverage() {
    const kqlQuery = `
    carTelemetry
    | where  dataRecordingTime > ago(7d)
    | where eventType == "SAMPLING" and isnotempty(h3Small) and signal == "gsmSignal" and signalValueDouble <> 99
    | summarize average = avg(signalValueDouble), max(signalValueDouble), min(signalValueDouble) by h3Small
    | project h3_hash_polygon = geo_h3cell_to_polygon(h3Small), telemetry = pack_all(), h3Small
    | project feature=pack(
            "type", "Feature",
            "geometry", h3_hash_polygon,
            "properties", telemetry)
    | summarize features = make_list(feature)
    | project pack(
            "type", "FeatureCollection",
            "features", features)    
    `;
    return query(kqlQuery);
}

// Count of events per location - creates a heatmap
// this query gets all "SAMPLING" event types and groups them, but only if the GPS "Fix" is good (above 2
module.exports.queryLocationHeatmap = async function queryLocationHeatmap() {
    const kqlQuery = `
    carTelemetry
    | where  dataRecordingTime > ago(7d)
    | where eventType == "SAMPLING" and isnotempty(h3Small) and signal == "fixType" and signalValueDouble >= 2
    | summarize eventCount = count() by h3Small
    | project h3_hash_polygon = geo_h3cell_to_polygon(h3Small), telemetry = pack_all(), h3Small
    | project feature=pack(
            "type", "Feature",
            "geometry", h3_hash_polygon,
            "properties", telemetry)
    | summarize features = make_list(feature)
    | project pack(
            "type", "FeatureCollection",
            "features", features)                
    `;
    return query(kqlQuery);
}

// Counts HARSH events per location (h3_small) to create a risk map
module.exports.queryHarshEvents = async function queryHarshEvents(){
    const kqlQuery = `
    carTelemetry
    | where  dataRecordingTime > ago(7d)
    | where eventType in ("HARSH", "HARSH_ACC", "HARSH_BRK")
    | where  isnotempty(h3Small) and signal == "fixType" and signalValueDouble >= 2
    | summarize
        eventCount = count(), 
        harsh = countif(eventType == "HARSH"), 
        harsh_acc = countif(eventType == "HARSH_ACC"), 
        harsh_brk = countif(eventType == "HARSH_BRK") 
        by h3Small  
    | project h3_hash_polygon = geo_h3cell_to_polygon(h3Small), telemetry = pack_all(), h3Small
    | project feature=pack(
            "type", "Feature",
            "geometry", h3_hash_polygon,
            "properties", telemetry)
    | summarize features = make_list(feature)
    | project pack(
            "type", "FeatureCollection",
            "features", features)
    `;
    return query(kqlQuery);
}

module.exports.queryTrips = async function queryTrips() {
    const kqlQuery = `
    Trips()
    | join kind=inner vehicleinfo on $left.vin == $right.['VIN Number']
    | extend vendor = iff(['Fuel Type'] == "Gas", 0, 1)
    | project vendor, list_position, list_relativeTimestap
    | project trips = bag_pack("vendor", vendor, "path", list_position, "timestamps", list_relativeTimestap)
    | summarize make_list(trips)
    `;
    return query(kqlQuery);
}

async function query(query) {
    try {

        // providing ClientRequestProperties
        // for a complete list of ClientRequestProperties
        // go to https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
        let clientRequestProps = new ClientRequestProperties();
        const oneMinute = 1000 * 60;
        clientRequestProps.setTimeout(oneMinute);

        // having client code provide its own clientRequestId is
        // highly recommended. It not only allows the caller to
        // cancel the query, but also makes it possible for the Kusto
        // team to investigate query failures end-to-end:
        clientRequestProps.clientRequestId = `FleetVisualization.Query;${uuidv4()}`;

        const results = await kustoClient.execute(database, query, clientRequestProps);

        return results;

    }
    catch (error) {
        console.log(error);
    }


}