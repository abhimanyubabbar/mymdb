(function(){

  angular.module('mymdb')
      .service('MyMDBService',['$log','$http','$q', MyMDBService]);

  function MyMDBService($log, $http, $q){

    $log.debug('main service initialized');

    function ping(){

      var deferred = $q.defer();
      $http({
        method: 'GET',
        url: '/ping'
      })
          .then(function(response){
            $q.resolve(response.data);
          }, function(err){
            $log.error(err);
            $q.reject('unable to ping the service: ' + err);
          });

      return deferred.promise;
    }

    return {
      ping : ping
    }
  }
})();