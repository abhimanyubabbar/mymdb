(function(){

  var express = require('express');
  var mainRouter = express.Router();

  mainRouter.get('/ping', function(req, res){
    res.json({service : "mymdb", response: "pong"});
  });

  module.exports = mainRouter;

})();