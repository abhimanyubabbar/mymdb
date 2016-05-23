(function(){

  var express = require('express');
  var healthRouter = express.Router();

  healthRouter.get('/ping', function(req, res){
    res.status(200).json({service:'mymdb', response:'pong!'});
  });


  module.exports = healthRouter;

})();