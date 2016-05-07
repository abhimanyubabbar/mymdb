(function(){

  // TODO : Create a component name and
  // TODO : use it in logging. Also get good logger.

  'use strict';

  var db = require('../models/database');
  var file = require('../models/fileProcess');
  var Q = require('q');

  // back-pressure.
  var bufferCount = 5000;
  var buffer= [];
  var dataPushed = false;

  /**
   * loadData: Start loading the information
   * in the system.
   *
   * @returns {*|promise}
   */
  function initDataStore(location){

    var deferred = Q.defer();
    var readStream  = file.createObjectReadStream(location);

    readStream.on('data', _dataHandler)
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
      console.log('remaining count' + buffer.length);
      return deferred.resolve('data load complete');
    }

    /**
     * handle information.
     * @param data
     * @private
     */
    function _dataHandler(data){

      // FIX ME : This method is not bullet proof
      // as it requires entry count to be multiple of bufferCount.
      // Needs to handle the remaining data when we have
      if(buffer.length >= bufferCount){

        if (!dataPushed) {

          console.log('buffering complete, going to add new movies now.');

          // pause the stream for
          // any more data updates.
          readStream.pause();
          dataPushed = true;

          console.log('paused the stream now, might get some events in between.');

          // add the information in the
          // application.
          db.addMovies(buffer)
              .then(function(){
                // once current batch goes through,
                // be ready to receive and push more data.
                console.log('add movie batch successful, resuming the stream.');
                readStream.resume();
                dataPushed = false;
              })
              .catch(function(err){
                // fail in case any element in
                // batch fails.
                console.log('movie batch addition failed, error: ' + err.toString());
                _errorHandler(err);

              });

          buffer = [];
        }
      } else {
        buffer.push(data);
      }
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