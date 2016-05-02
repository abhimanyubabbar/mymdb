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

// perform mandatory initialization
// which at this moment involves the db init().
db.init()
    .then(function(result){
      app.listen(3000, function(){
        console.log('server started');
      });
    }, function(err){
      console.log('unable to start the server as mandatory init failed.');
      console.log(err);
    });
