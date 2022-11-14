// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
var map, datasource, popup;
var geoJSONdata;

var defaultColor = "#fafa6e";

var colorScaleDefault = [
    0, "#fafa6e",
    10, '#cdde58',
    20, '#a1c144',
    30, '#77a531',
    40, '#4e891f',
    50, '#216e0d'
];

document.body.onload = GetMap();

function GetMap() {
    //Initialize a map instance.
    map = new atlas.Map('myMap', {
        
        center: [-122.12, 47.64],
        zoom: 11,

        //Pitch the map so that the extrusion of the polygons is visible.
        pitch: 45,

        style: 'grayscale_dark',

        showBuildingModels: true,

        view: 'Auto',
        
        //Add authentication details for connecting to Azure Maps.
        authOptions: {
            //Alternatively, use an Azure Maps key. Get an Azure Maps key at https://azure.com/maps. NOTE: The primary key should be used as the key.
            authType: 'subscriptionKey',
            subscriptionKey: '<add your key here>'
        }
    });

    //Wait until the map resources are ready.
    map.events.add('ready', function () {
        //Add the Style Control to the map.
        map.controls.add(new atlas.control.StyleControl({
            //Optionally specify which map styles you want to appear in the picker. 
            //All styles available with the S0 license tier appear by default in the control. 
            //If using a S1 tier license, you can use the mapStyles option to add the 'satellite' and 'satellite_road_labels' styles to the control.
            mapStyles: ['road', 'road_shaded_relief', 'grayscale_light', 'night', 'grayscale_dark', 'satellite', 'satellite_road_labels']
        }), {
            position: "top-right"
        });

        //Create a popup but leave it closed so we can update it and display it later.
        popup = new atlas.Popup({
            position: [0, 0]
        });

        // Hide when finish
        document.getElementById('loadingIcon').style.display = 'none';
    });
}

function createExtrusion(dataurl, datapoint, colorScale){

    datasource = new atlas.source.DataSource();
    datasource.importDataFromUrl(dataurl).then(() => {
        //Hide the loading icon.
        document.getElementById('loadingIcon').style.display = 'none';
    });

    map.sources.add(datasource);

    //Create a stepped expression based on the color scale.
    var steppedExp = [
        'step',
        ['get', datapoint],
        defaultColor
    ];
    steppedExp = steppedExp.concat(colorScale);

    //Create an interpolate expression for height based on the `point_count` value of each cell. 
    var heightExp = [
        'interpolate',
        ['linear'],
        ['get', datapoint],
        1, 10,      
        50, 200, 
        100, 300,
        200, 400
    ];

    //Create a polygon extrusion layer to render all cells of a grid with some opacity.
    var polygonLayer = new atlas.layer.PolygonExtrusionLayer(datasource, null, {
        fillColor: steppedExp,
        fillOpacity: 0.6,
        height: heightExp
    });            

    //Create a second polygon extrusion layer to use a to highlight hovered grid cells by giving them a solid opacity.
    var polygonHoverLayer = new atlas.layer.PolygonExtrusionLayer(datasource, null, {
        fillColor: steppedExp,
        fillOpacity: 1,
        height: heightExp,

            //Only polygons with a "h3Small" property with a value of '' will be rendered.
            filter: ['==', ['get', 'h3Small'], '']
    });

    //Add a click event to the layer.
    map.events.add('click', polygonLayer, featureClicked);

    //Add polygon and line layers to the map, below the labels..
    map.layers.add([
        polygonLayer, 
        polygonHoverLayer
    ], 'labels');

    // When the user moves their mouse over the polygonLayer, we'll update the filter in
    // the polygonHoverLayer to only show the matching state, thus creating a hover effect.
    map.events.add('mousemove', polygonLayer, function (e) {
        polygonHoverLayer.setOptions({ filter: ['==', ['get', 'h3Small'], e.shapes[0].getProperties().h3Small] });
        map.getCanvasContainer().style.cursor = 'pointer';
    });

    // Reset the polygonHoverLayer layer's filter when the mouse leaves the layer.
    map.events.add('mouseleave', polygonLayer, function (e) {
        polygonHoverLayer.setOptions({ filter: ['==', ['get', 'h3Small'], ''] });
        map.getCanvasContainer().style.cursor = 'grab';
    });
}

function createPolygon(dataurl, datapoint, colorScale){

    datasource = new atlas.source.DataSource();
    datasource.importDataFromUrl(dataurl).then(() => {
        //Hide the loading icon.
        document.getElementById('loadingIcon').style.display = 'none';
    });;
    map.sources.add(datasource);

    //Create a stepped expression based on the color scale.
    var steppedExp = [
        'step',
        ['get', datapoint],
        defaultColor
    ];
    steppedExp = steppedExp.concat(colorScale);

    //Create a polygon layer to render all cells of a grid with some opacity.
    var polygonLayer = new atlas.layer.PolygonLayer(datasource, null, {
        fillColor: steppedExp,
        fillOpacity: 0.6
    });            

    //Add a click event to the layer.
    map.events.add('click', polygonLayer, featureClicked);

    //Add polygon and line layers to the map, below the labels..
    map.layers.add([
        polygonLayer
    ], 'labels');

}


function featureClicked(e) {
    //Make sure the event occurred on a shape feature.
    if (e.shapes && e.shapes.length > 0) {
        //By default, show the popup where the mouse event occurred.
        var pos = e.position;
        var offset = [0, 0];
        var properties;

        if (e.shapes[0] instanceof atlas.Shape) {
            properties = e.shapes[0].getProperties();

            //If the shape is a point feature, show the popup at the points coordinate.
            if (e.shapes[0].getType() === 'Point') {
                pos = e.shapes[0].getCoordinates();
                offset = [0, -18];
            }
        } else {
            properties = e.shapes[0].properties;

            //If the shape is a point feature, show the popup at the points coordinate.
            if (e.shapes[0].type === 'Point') {
                pos = e.shapes[0].geometry.coordinates;
                offset = [0, -18];
            }
        }

        //Update the content and position of the popup.
        popup.setOptions({
            //Create a table from the properties in the feature.
            content: atlas.PopupTemplate.applyTemplate(properties),
            position: pos,
            pixelOffset: offset
        });

        //Open the popup.
        popup.open(map);
    }
}