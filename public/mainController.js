(function(){

  var mymdb = angular.module('mymdb');
  mymdb.controller('MainController', ['$log','MyMDBService', MainController]);

  function MainController($log, MyMDBService){

    var self = this;
    $log.debug('main-controller up ..');

    function init () {

      MyMDBService.ping()
          .then(function(data){
            $log.debug('service seems to be running');
            $log.debug(data);
          })
          .catch(function(err){
            $log.error('whaaaaatt ??');
          });
    }

    init();
  }
})();