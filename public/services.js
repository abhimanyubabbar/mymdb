(function(){

  angular.module('mymdb')
      .service('MyMDBService',['$log','$http','$q', MyMDBService]);

  function MyMDBService($log, $http, $q){

    $log.debug('main service initialized');

    /**
     * ping : service health check.
     * @returns {*|promise}
     */
    function ping(){

      var deferred = $q.defer();

      $http({
        method: 'GET',
        url: '/health/ping'
      })
          .then(function(response){
            deferred.resolve(response.data);
          })
          .catch(function(err){
            $log.error(err);
            deferred.reject('unable to ping the service: ' + err);
          });

      return deferred.promise;
    }


    /**
     * filteredMovies : Based on the filtering params, fetch the
     * movies from the database.
     *
     * @param params
     * @returns {*|promise}
     */
    function filteredMovies(params) {

      var deferred = $q.defer();

      $http({
        method: 'GET',
        url: '/movies',
        params: params
      })
          .then(function(response){
            deferred.resolve(response.data)
          })
          .catch(function(err){
            deferred.reject({error: 'unable to fetch filtered movies', reason: err})
          });

      return deferred.promise;
    }


    /**
     * topRatedByCountry : fetch the top rated movies based on the
     * country filter.
     *
     * @param params
     */
    function topRatedByCountry(params) {

      var deferred = $q.defer();

      $http({
        method: 'GET',
        url: '/movies/topRated100ByCountry',
        params: params
      })
          .then(function(response){
            deferred.resolve(response.data)
          })
          .catch(function(err){
            deferred.reject({error: 'unable to fetch top rated movies by country', reason: err})
          });

      return deferred.promise;

    }

    return {
      ping : ping,
      filteredMovies : filteredMovies,
      topRatedByCountry: topRatedByCountry
    }
  }
})();