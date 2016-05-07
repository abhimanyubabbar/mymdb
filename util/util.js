(function(){

  // TODO : Create a component name and
  // TODO : use it in logging. Also get good logger.

  'use strict';

  var db = require('../models/database');
  var file = require('../models/fileProcess');
  var Q = require('q');


  /**
   * loadData: Start loading the information
   * in the system.
   *
   * @returns {*|promise}
   */
  function initDataStore(location){

    var deferred = Q.defer();
    var readStream  = file.createObjectReadStream(location);

    readStream.on('data', _loadDataHandler)
        .on('close', _closeHandler)
        .on('error', _errorHandler);

    /**
     * _errorHandler : handling
     * errors from the streams.
     * @param error
     * @private
     */
    function _errorHandler(error) {
      readStream.removeListener('data', _loadDataHandler);
      deferred.reject(new Error(error));
    }

    /**
     * _closeHandler : handle the closing of the
     * data stream.
     * @private
     */
    function _closeHandler(){
      deferred.resolve('data load complete');
    }

    /**
     * _loadDataHandler : Load the information passed
     * in to the data store exposed by db.
     *
     * @param obj
     * @private
     */
    function _loadDataHandler(obj){

      db.addMovie(obj)
          .catch(function(err){
            _errorHandler(err);
          });
    }

    return deferred.promise;
  }

  module.exports = {
    initDataStore : initDataStore
  };
})();