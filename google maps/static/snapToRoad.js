// 路径可能需要修改
// 读取方法参照 https://gist.github.com/derzorngottes/3b57edc1f996dddcab25
// var apiKey = config.gcp_api_key;
var apiKey = "AIzaSyAMbZhmkYUuM0RmICwJ1SFqQ0DknznpoRQ";

var map;
var drawingManager;
var placeIdArray = [];
var polylines = [];
var snappedCoordinates = [];
var lineIndex;

function loadMap() {
  var script = document.createElement("script");
  script.src = "https://maps.googleapis.com/maps/api/js?libraries=drawing,places&key=" + apiKey;
  document.getElementsByTagName("head")[0].appendChild(script);
}

function CenterControl(controlDiv) {
  // Set CSS for the control border.
  const controlUI = document.createElement("input");

  controlUI.style.backgroundColor = "#fff";
  controlUI.style.border = "2px solid #fff";
  controlUI.style.borderRadius = "3px";
  controlUI.style.boxShadow = "0 2px 6px rgba(0,0,0,.3)";
  controlUI.style.cursor = "pointer";
  controlUI.style.marginBottom = "22px";
  controlUI.style.fontSize = "16px";
  // controlUI.style.marginTop = "8px";
  // controlUI.style.textAlign = "center";
  controlUI.type = "datetime-local";
  controlUI.name = "local-time";
  controlUI.id = "specific";
  controlUI.title = "Click to select the specific date";
  controlUI.pattern = "yyyy-MM-dd HH:mm";
  controlDiv.appendChild(controlUI);

  // Setup the click event listeners: simply set the map to Chicago.
  controlUI.addEventListener("click", () => {
    
  });
}

function initialize() {
  var mapOptions = {
    zoom: 17,
    center: {lat: 40.7724, lng: -73.9841,}
  };
  map = new google.maps.Map(document.getElementById('map'), mapOptions);

  const dateControlDiv = document.createElement("div");
  CenterControl(dateControlDiv);
  map.controls[google.maps.ControlPosition.BOTTOM_CENTER].push(dateControlDiv);

  setInterval(getTime, 1000);
  process_time();

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
  // drawingManager = new google.maps.drawing.DrawingManager({
  //   drawingMode: google.maps.drawing.OverlayType.POLYLINE,
  //   drawingControl: true,
  //   drawingControlOptions: {
  //     position: google.maps.ControlPosition.TOP_CENTER,
  //     drawingModes: [
  //       google.maps.drawing.OverlayType.POLYLINE
  //     ]
  //   },
  //   polylineOptions: {
  //     strokeColor: '#696969',
  //     strokeWeight: 2,
  //     strokeOpacity: 0.3,
  //   }
  // });
  // drawingManager.setMap(map);

  // Snap-to-road when the polyline is completed.
  // drawingManager.addListener('polylinecomplete', function(poly) {
  //   var path = poly.getPath();
  //   polylines.push(poly);
  //   placeIdArray = [];
  //   runSnapToRoad(path);
  // });

  placeIdArray = [];
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
  var limit = 100; // The upper limit of the number of points we need to draw each time
  for(var i = 0; i < len; i++) {
    lineIndex = i;
    var index = 0 ;
    pathValues = [];
    rLen = result[i].length;
    // console.log(rLen);
    while (index <= rLen / limit) {
      console.log(i);
      if (rLen < (index + 1) * limit) {
        pathValues = result[i].slice(index * limit, rLen);
      }
      pathValues = result[i].slice(index * limit, (index + 1) * limit);
      // console.log(pathValues);
      // process_data();
      index++;

      $.get('https://roads.googleapis.com/v1/snapToRoads', {
        interpolate: true,
        key: apiKey,
        path: pathValues.join('|'),
      }, function(data) {
        processSnapToRoadResponse(data);
        drawSnappedPolyline();
      });
    }
  }
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

function sleep(delay) {
  var start = (new Date()).getTime();
  while ((new Date()).getTime() - start < delay) {
    continue;
  }
}

// Draws the snapped polyline (after processing snap-to-road response).
function drawSnappedPolyline() {
  var gradient = new gradientColor("#00FF00","#FF0000", 100);
  // console.log(gradient);

  var step = 0;
  console.log(lineIndex);
  if (rating[lineIndex] < -500) {
    step = 100;
  } else {
    step -= rating[lineIndex] / 5;
  }

  var snappedPolyline = new google.maps.Polyline({
    path: snappedCoordinates,
    // strokeColor: '#00FF00',
    strokeColor: gradient[step],
    strokeWeight: 4,
    strokeOpacity: 0.9,
  });

  snappedPolyline.setMap(map);
  polylines.push(snappedPolyline);
}

function getTime() {
  var today = new Date();
  var yyyy = today.getFullYear();
  var MM = today.getMonth() + 1;
  var dd = today.getDate();
  var hh = today.getHours();
  var mm = today.getMinutes();
  MM = checkTime(MM);
  dd = checkTime(dd);
  hh = checkTime(hh);
  mm = checkTime(mm);
  var time = yyyy + "-" + MM + "-" + dd + "T" + hh + ":" + mm;

  var dateControl = document.querySelector("#specific");
  dateControl.value = time;

  function checkTime(i) {
      if (i < 10) {
          i = "0" + i;
      }
      return i;
  }
}

// gradient color setting
class gradientColor {
  constructor(startColor, endColor, step) {
    var startRGB = this.colorRgb(startColor); // Convert to rgb array mode
    var startR = startRGB[0];
    var startG = startRGB[1];
    var startB = startRGB[2];

    var endRGB = this.colorRgb(endColor);
    var endR = endRGB[0];
    var endG = endRGB[1];
    var endB = endRGB[2];

    var sR = (endR - startR) / step; // Final difference
    var sG = (endG - startG) / step;
    var sB = (endB - startB) / step;

    var colorArr = [];
    for (var i = 0; i < step; i++) {
      // Calculate the hex value of each step
      var hex = this.colorHex('rgb(' + parseInt((sR * i + startR)) + ',' + parseInt((sG * i + startG)) + ',' + parseInt((sB * i + startB)) + ')');
      colorArr.push(hex);
    }
    return colorArr;
  }
  colorRgb(sColor) {
    var reg = /^#([0-9a-fA-f]{3}|[0-9a-fA-f]{6})$/;
    var sColor = sColor.toLowerCase();
    if (sColor && reg.test(sColor)) {
      if (sColor.length === 4) {
        var sColorNew = "#";
        for (var i = 1; i < 4; i += 1) {
          sColorNew += sColor.slice(i, i + 1).concat(sColor.slice(i, i + 1));
        }
        sColor = sColorNew;
      }
      //处理六位的颜色值
      var sColorChange = [];
      for (var i = 1; i < 7; i += 2) {
        sColorChange.push(parseInt("0x" + sColor.slice(i, i + 2)));
      }
      return sColorChange;
    } else {
      return sColor;
    }
  }
  colorHex(rgb) {
    var _this = rgb;
    var reg = /^#([0-9a-fA-f]{3}|[0-9a-fA-f]{6})$/;
    if (/^(rgb|RGB)/.test(_this)) {
      var aColor = _this.replace(/(?:(|)|rgb|RGB)*/g, "").split(",");
      var strHex = "#";
      for (var i = 0; i < aColor.length; i++) {
        var hex = Number(aColor[i]).toString(16);
        hex = hex < 10 ? 0 + '' + hex : hex; // 保证每个rgb的值为2位
        if (hex === "0") {
          hex += hex;
        }
        strHex += hex;
      }
      if (strHex.length !== 7) {
        strHex = _this;
      }
      return strHex;
    } else if (reg.test(_this)) {
      var aNum = _this.replace(/#/, "").split("");
      if (aNum.length === 6) {
        return _this;
      } else if (aNum.length === 3) {
        var numHex = "#";
        for (var i = 0; i < aNum.length; i += 1) {
          numHex += (aNum[i] + aNum[i]);
        }
        return numHex;
      }
    } else {
      return _this;
    }
  }
}

function process_time() {
  // $.ajax({
  //   url: '/Mission/editMission?missionId=' + data,
  //   type: 'post',
  //   dataType: 'json',
  //   success: function (result) {
  //     var time1 = result.missionStartTime.toString().replace(' ', 'T');
  //     $("#specific").val(time1);
  //   },
  //   error: function (data) {
  //     console.log(data);
  //   }
  // });
}

// function process_data() {
//   $.ajax({
//     type: "GET",
//     url: "../../points.csv",                
//     dataType : "text",
//     contentType: "application/json; charset=utf-8",
//     success: function(){
//       var data = $.csv.toObjects("points.csv");
//       console.log(data);
//     },
//     error: function(error){
//         console.log("Error");
//         console.log(error);
//     }
//   });
// }


window.onload = loadMap();
$(window).load(initialize);