(function(){

  angular.module('mymdb')
      .controller('LandingController',['$log', LandingController]);

  function LandingController($log){

    $log.debug('landing controller up ...');
    var self = this;

    function init(self){
      self.resources = ['./resources/HeyNow.mp4'];
      self.poster = './resources/hey_now_poster.png';
      self.fullScreen = true;
      self.muted = true;
      self.zIndex = '22';
      self.playInfo = {};
      self.pausePlay = true;
    }


    init(self);
  }

})();