var express = require('express');
var bodyParser = require('body-parser');
var db = require('./models/database');
var helper = require('./util/util');


var app = express();

// Middleware.
app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());


// MyMdb API

// TODO : extract the api in a separate module.
// TODO : /server/routes
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
        resp.send('movie added successfully.')
      })
      .catch(function(err){
        resp.send('unable to add the movie in system');
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

(function(location){

  console.log('started with initializing the database.');
  db.init()
      .then(function(){
        console.log('database up, initializing the datastore now ..');
        return helper.initDataStore(location)
      })
      .then(function(){
        console.log('data store initialized, creating server now ..');
        app.listen(3000, function(){console.log('server booted up.')});
      })
      .catch(function(){
        console.log('unable to initialize thd database.');
      })
})('./resources/movies-copy.list');
