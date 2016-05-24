(function(){

  var mymdb = angular.module('mymdb');
  mymdb.controller('MainController', ['$log','MyMDBService', MainController]);

  function MainController($log, MyMDBService){

    var self = this;
    $log.debug('main-controller up ..');

    /**
     * init : variables initialization.
     * @param self
     */
    function init (self) {

      self.filter = {
        min_rating: 0.0
      };

      self.topRatedParams = {};

      self.filteredMovies = [];
      self.topRatedMovies = [];
    }

    /**
     * fetch movies based on filtering params.
     * @param params
     */
    self.getFilteredMovies = function(params) {

      MyMDBService.filteredMovies(params)
          .then(function (data) {
            // assign the information to the filtered movies obj.
            self.filteredMovies = data.movies;
          })
          .catch(function (err) {
            console.log(err);
          })
    };

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