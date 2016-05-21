var express = require('express');
var bodyParser = require('body-parser');
var db = require('./models/database');
var helper = require('./util/util');

var bunyan = require('bunyan');
var logger = bunyan.createLogger({name:"main"});

var app = express();
var movieRoute = require('./server/movies-route');
var mainRoute = require('./server/main-route');

// Middleware.
app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());

// MyMdb API
app.use('/', mainRoute);
app.use('/movies', movieRoute);

(function(location){

  logger.info('started with initializing the database.');

  //db.init()
   //   .then(function(){
    //    logger.info('database up, initializing the data store now ..');
   //     return helper.dataStore(location)
   //   })
   //   .then(function(){
   //     logger.debug('data store initialized, creating server now ..');
    //    app.listen(3000, function(){logger.info('server booted up.')});
    //  })
     // .catch(function(err){
      //  logger.error({err:err},'unable to initialize thd database.');
     // })

  db.init()
      .then(function(){

        logger.info('database up, booting the server');
        app.listen(3000, function(){
          logger.info('server booted up ...');
        })
      })


})({movies: './resources/movies-copy.list', country : './resources/countries-copy.list', ratings: './resources/ratings-copy.list'});
