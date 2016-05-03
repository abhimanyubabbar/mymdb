var express = require('express');
var bodyParser = require('body-parser');
var db = require('./models/database');


var app = express();

// Middleware.
app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());


// Expose the API.

app.get('/ping', function(req, resp){
  resp.send('{"application":"mymdb", "status":"pong"}');
});

app.post('/movies', function(req, resp){
  console.log('received a call to add a new movie in the system.');
  var movie = {
    title: req.body.title,
    year: req.body.year
  };
  db.addMovie(movie)
      .then(function(result){
        console.log('movie added successfully');
        resp.send('movie added successfully.')
      }, function(err){
        console.log('woops .. ');
      });
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
