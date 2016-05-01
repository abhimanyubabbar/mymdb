var express = require('express');
var app = express();

var db = require('./models/database');

app.get('/ping', function(req, resp){
  resp.send('{"application":"mymdb", "status":"pong"}');
});

app.get('/movies/count', function(req, resp){

  console.log('received a call to fetch the movies count');

  db.movieCount()
      .then(function( val){
        resp.send('movie count:' + val);
      }
      , function(err){
        console.log(err);
        resp.send('unable to fulfill the request');
      })
});

app.listen(3000, function(){
  console.log('server started');
});