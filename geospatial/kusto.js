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

module.exports.queryCellPhoneCoverage = async function queryCellPhoneCoverage() {
    const kqlQuery = `GeoCellPhoneCoverage()`;
    return query(kqlQuery);
}

module.exports.queryAverageSpeed = async function queryAverageSpeed(){
    const kqlQuery = `GeoFleetAverageSpeed()`;
    return query(kqlQuery);
}

module.exports.queryLocationHeatmap = async function queryLocationHeatmap() {
    const kqlQuery = `GeoFleetHeatmap()`;
    return query(kqlQuery);
}

module.exports.queryEvent = async function queryEvent(eventType) {    
    const kqlQuery = `GeoDrivingEvent('7d', '${eventType}')`;
    return query(kqlQuery);
}

module.exports.queryHarshEvents = async function queryHarshEvents(){
    const kqlQuery = `
    GeoHarshEvents
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


module.exports.queryLocalWeather = async function queryLocalWeather() {
    const kqlQuery = `GeoFleetLocalTemperature()`;
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