(function () {

  var mymdb = angular.module('mymdb');
  mymdb.controller('FuzzySearchController', ['$log', 'MyMDBService', FuzzySearchController]);

  function FuzzySearchController($log, MyMDBService) {

    $log.debug('fuzzy controller up and running');
    var self = this;

    /**
     * init : variables initialization.
     * @param self
     */
    function init(self) {

      self.filter = {
        min_rating: 0.0
      };

      self.filteredMovies = [];
    }

    /**
     * fetch movies based on filtering params.
     * @param valid
     * @param params
     */
    self.getFilteredMovies = function (valid, params) {

      if (!valid) {
        console.log('invalid form');
        return
      }

      MyMDBService.filteredMovies(params)
          .then(function (data) {
            // assign the information to the filtered movies obj.
            self.filteredMovies = data.movies;
          })
          .catch(function (err) {
            console.log(err);
          })
    };

    init(self);
  }
})();