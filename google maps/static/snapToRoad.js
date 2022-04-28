// 路径可能需要修改
var apiKey = 'config.gcp_api_key';

var map;
var drawingManager;
var placeIdArray = [];
var polylines = [];
var snappedCoordinates = [];

function initialize() {
  var mapOptions = {
    zoom: 17,
    center: {lat: -33.8667, lng: 151.1955,}
  };
  map = new google.maps.Map(document.getElementById('map'), mapOptions);

  // Adds a Places search box. Searching for a place will center the map on that
  // location.
  map.controls[google.maps.ControlPosition.RIGHT_TOP].push(
      document.getElementById('bar'));
  var autocomplete = new google.maps.places.Autocomplete(
      document.getElementById('autoc'));
  autocomplete.bindTo('bounds', map);
  autocomplete.addListener('place_changed', function() {
    var place = autocomplete.getPlace();
    if (place.geometry.viewport) {
      map.fitBounds(place.geometry.viewport);
    } else {
      map.setCenter(place.geometry.location);
      map.setZoom(17);
    }
  });

  // Enables the polyline drawing control. Click on the map to start drawing a
  // polyline. Each click will add a new vertice. Double-click to stop drawing.
  drawingManager = new google.maps.drawing.DrawingManager({
    drawingMode: google.maps.drawing.OverlayType.POLYLINE,
    drawingControl: true,
    drawingControlOptions: {
      position: google.maps.ControlPosition.TOP_CENTER,
      drawingModes: [
        google.maps.drawing.OverlayType.POLYLINE
      ]
    },
    polylineOptions: {
      strokeColor: '#696969',
      strokeWeight: 2,
      strokeOpacity: 0.3,
    }
  });
  drawingManager.setMap(map);

  // Snap-to-road when the polyline is completed.
//   drawingManager.addListener('polylinecomplete', function(poly) {
//     var path = poly.getPath();
//     polylines.push(poly);
//     placeIdArray = [];
//     runSnapToRoad(path);
//   });
  runSnapToRoad();

  // Clear button. Click to remove all polylines.
  document.getElementById('clear').addEventListener('click', function(event) {
    event.preventDefault();
    for (var i = 0; i < polylines.length; ++i) {
      polylines[i].setMap(null);
    }
    polylines = [];
    return false;
  });
}

// Snap a user-created polyline to roads and draw the snapped path
function runSnapToRoad() {
//   var pathValues = [];
//   for (var i = 0; i < path.getLength(); i++) {
//     pathValues.push(path.getAt(i).toUrlValue());
//   }

  var pathValues = ['-33.865278,151.193923', '-33.866507,151.194567', '-33.866685,151.195629', '-33.867968,151.196112', '-33.868342,151.196691'];
  console.log(pathValues);

  $.get('https://roads.googleapis.com/v1/snapToRoads', {
    interpolate: true,
    key: apiKey,
    path: pathValues.join('|')
  }, function(data) {
    processSnapToRoadResponse(data);
    drawSnappedPolyline();
  });
}

// Store snapped polyline returned by the snap-to-road service.
function processSnapToRoadResponse(data) {
  snappedCoordinates = [];
  placeIdArray = [];
  for (var i = 0; i < data.snappedPoints.length; i++) {
    var latlng = new google.maps.LatLng(
        data.snappedPoints[i].location.latitude,
        data.snappedPoints[i].location.longitude);
    snappedCoordinates.push(latlng);
    placeIdArray.push(data.snappedPoints[i].placeId);
  }
}

// Draws the snapped polyline (after processing snap-to-road response).
function drawSnappedPolyline() {
  var snappedPolyline = new google.maps.Polyline({
    path: snappedCoordinates,
    strokeColor: '#FF8C00',
    strokeWeight: 4,
    strokeOpacity: 1.0,
  });

  snappedPolyline.setMap(map);
  polylines.push(snappedPolyline);
}

$(window).load(initialize);