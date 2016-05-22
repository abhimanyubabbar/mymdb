(function(){

  var mymdb = angular.module('mymdb', ['ngVidBg', 'ngRoute'])
      .config(['$logProvider','$routeProvider', function($logProvider, $routeProvider){

        // configure the log provider
        $logProvider.debugEnabled(true);

        // setup the basic routes in the system
        $routeProvider
            .when('/', {
              templateUrl: './templates/landing.html'
            })
            .when('/query', {
              templateUrl: 'templates/query.html'
            })
      }])

})();