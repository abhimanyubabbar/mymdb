(function(){

  var express = require('express');
  var mainRouter = express.Router();
  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name: 'main-route'});


  mainRouter.get('/', function(req, res){
    res.render('index.html');
  });

  mainRouter.get('/ping', function(req, res){
    logger.info('call to ping endpoint');
    res.json({service : "mymdb", response: "pong"});
  });

  module.exports = mainRouter;

})();