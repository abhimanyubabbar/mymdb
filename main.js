var express = require('express');
var bodyParser = require('body-parser');
var db = require('./models/database');
var helper = require('./util/util');

var bunyan = require('bunyan');
var logger = bunyan.createLogger({name:"main"});

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

app.get('/movies/count', function(req, resp){

  logger.info('received a call to fetch the movies count');

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

  logger.debug('started with initializing the database.');

  db.init()
      .then(function(){
        logger.debug('database up, initializing the datastore now ..');
        return helper.dataStore(location)
      })
      .then(function(){
        logger.debug('data store initialized, creating server now ..');
        app.listen(3000, function(){logger.info('server booted up.')});
      })
      .catch(function(err){
        logger.error({err:err},'unable to initialize thd database.');
      })
})({movies: './resources/movies-extract.list', country : './resources/countries-extract.list'});
