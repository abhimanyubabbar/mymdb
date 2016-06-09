(function(){

  var mymdb = angular.module('mymdb');
  mymdb.controller('TopRatedController', ['$log','MyMDBService', TopRatedController]);

  function TopRatedController($log, MyMDBService){

    var self = this;
    $log.debug('main-controller up ..');

    /**
     * init : variables initialization.
     * @param self
     */
    function init (self) {

      self.topRatedParams = {};
      self.topRatedMovies = [];
    }

    /**
     * Get Me the top Rated movies in a particular country.
     * @param params
     */
    self.getTopRatedMovies = function(params){

      MyMDBService.topRatedByCountry(params)
          .then(function(data){
            self.topRatedMovies = data.movies;
          })
          .catch(function(err){
            console.log(err);
          })
    };

    init(self);
  }
})();