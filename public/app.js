(function(){

  var mymdb = angular.module('mymdb', ['ngVidBg', 'ngRoute'])
      .config(['$logProvider','$routeProvider', function($logProvider, $routeProvider){

        // configure the log provider
        $logProvider.debugEnabled(true);

        // setup the basic routes in the system
        $routeProvider
            .when('/', {
              templateUrl: 'templates/video.html'
            })
            .when('/landing',{
              templateUrl:'templates/landing.html'
            })
            .when('/rated-query', {
              templateUrl: 'templates/top-rated-query.html'
            })
            .when('/fuzzy-query', {
              templateUrl: 'templates/fuzzy-query.html'
            })
      }])

})();