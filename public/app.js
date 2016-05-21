(function(){

  var mymdb = angular.module('mymdb', [])
      .config(['$logProvider', function($logProvider){
        $logProvider.debugEnabled(true);
      }])

})();