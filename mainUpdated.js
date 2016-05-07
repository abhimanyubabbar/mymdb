var express = require('express');
var bodyParser = require('body-parser');
var db = require('./models/database');
var helper = require('./util/util');


var app = express();

// Middleware.
//app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());


// MyMdb API

// TODO : extract the api in a separate module.
// TODO : /server/routes
app.get('/ping', function(req, resp){
  resp.send('{"application":"mymdb", "status":"pong"}');
});

app.post('/movies', function(req, resp){
  console.log('received a call to add a new movie in the system.');
  console.log(req.body);

  var movie = {
    title: req.body.title,
    year: req.body.year
  };
  console.log(movie);

  db.addMovie(movie)
      .then(function(result){
        resp.send('movie added successfully.')
      })
      .catch(function(err){
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


(function(){

  console.log('started with initializing the database.');
  db.init()
      .then(function(){
        app.listen(3000)
            .then(function(){
              console.log('server started')
            })

      })

})();

