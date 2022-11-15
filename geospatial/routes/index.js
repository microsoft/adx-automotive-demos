// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
require('dotenv').config();
var express = require('express');
var router = express.Router();
const kusto = require('../kusto.js');
const mapKey = process.env.AZURE_MAPS_KEY;

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', { title: 'Fleet Analytics' });   
});

router.get('/safety', function(req, res, next) {
  res.render('safety', { title: 'Safety', key: mapKey  });   
});

router.get('/heatmap', function(req, res, next) {
  res.render('heatmap', { title: 'Heatmap', key: mapKey });   
});

router.get('/coverage', function(req, res, next) {
  res.render('coverage', { title: 'Coverage', key: mapKey  });   
});

router.get('/geospatial/cell_coverage', function(req, res, next) {
  kusto.queryCellPhoneCoverage().then(result => {     
    var geoJSON = result.primaryResults[0][0].raw[0];
    res.json(geoJSON);    
  });
});

router.get('/geospatial/location_heatmap', function(req, res, next) {
  kusto.queryLocationHeatmap().then(result => {   
    var geoJSON = result.primaryResults[0][0].raw[0];
    res.json(geoJSON);
  });
});

router.get('/geospatial/harsh_event', function(req, res, next) {
  kusto.queryHarshEvents().then(result => {   
    var geoJSON = result.primaryResults[0][0].raw[0];
    res.json(geoJSON);
  });
});

router.get('/geospatial/trips', function(req, res, next) {
  kusto.queryTrips().then(result => {   
    var geoJSON = result.primaryResults[0][0].raw[0];
    res.json(geoJSON);
  });
});

module.exports = router;
