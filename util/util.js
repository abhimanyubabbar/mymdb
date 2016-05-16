(function () {

  'use strict';

  var db = require('../models/database');
  var file = require('../models/fileProcess');
  var fs = require('fs');
  var Q = require('q');
  var SeparatorChunker = require('chunking-streams').SeparatorChunker;
  var pipes = require('./pipes');

  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name: "util"});

  // back-pressure.
  var bufferCount = 5000;
  var buffer = [];
  var dataPushed = false;

  /**
   * loadData: Start loading the information
   * in the system.
   *
   * @returns {*|promise}
   */
  function initDataStore(location) {

    var deferred = Q.defer();
    var readStream = file.createObjectReadStream(location);

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
    function _closeHandler() {
      logger.debug('remaining count' + buffer.length);
      return deferred.resolve('data load complete');
    }

    /**
     * handle information.
     * @param data
     * @private
     */
    function _dataHandler(data) {

      // FIX ME : This method is not bullet proof
      // as it requires entry count to be multiple of bufferCount.
      // Needs to handle the remaining data when we have
      if (buffer.length >= bufferCount) {

        if (!dataPushed) {

          logger.debug('buffering complete, going to add new movies now.');

          // pause the stream for
          // any more data updates.
          readStream.pause();
          dataPushed = true;

          logger.debug('paused the stream now, might get some events in between.');

          // add the information in the
          // application.
          db.addMovies(buffer)
              .then(function () {
                // once current batch goes through,
                // be ready to receive and push more data.
                logger.debug('add movie batch successful, resuming the stream.');
                readStream.resume();
                dataPushed = false;
              })
              .catch(function (err) {
                // fail in case any element in
                // batch fails.
                logger.error({err: err.toString()}, 'movie batch addition failed, error');
                _errorHandler(err);

              });

          buffer = [];
        }
      } else {
        buffer.push(data);
      }
    }

    return deferred.promise;
  }

  /**
   * dataStore : init the datastore for the service
   * in the system.
   *
   * @param location
   */
  function dataStore(location) {

    var deferred = Q.defer();

    _loadMovies(location.movies)
        .then(function () {
          logger.info('completed with loading of movies, moving to country loading.');
          return _loadCountry(location.country);
        })
        .then(function () {
          logger.info('loaded the country information successfully');
          deferred.resolve('information loaded successfully.');
        })
        .catch(function (err) {
          deferred.reject(err);
        });

    return deferred.promise;
  }


  /**
   * _loadMovies : Load the main movies information
   * in the system.
   *
   * @param movieDumpLoc
   * @returns {*|promise}
   * @private
   */
  function _loadMovies(movieDumpLoc) {

    var deferred = Q.defer();

    var rs = fs.createReadStream(movieDumpLoc, {encoding: 'utf8'});

    // TO DO : Add a separate destroy function to
    // properly close and release all the resources.
    rs.pipe(new SeparatorChunker({
      separator: '\n',
      flushTail: false
    }))
        .pipe(new pipes.TrimMe())
        .pipe(new pipes.Split(/\t{1,}/))
        .pipe(new pipes.MovieFilter())
        .pipe(new pipes.TrimMe()) //trim the individual properties.
        .pipe(new pipes.Batcher(bufferCount))
        .pipe(new pipes.MovieDbBatchWriter())
        .on('finish', function () {
          deferred.resolve('finished loading the movies information.');
        })
        .on('error', function (err) {
          deferred.reject(err);
        });

    return deferred.promise;
  }


  /**
   * _loadCountry : Update the movies with the location of
   * the country.
   * @param countryDumpLoc
   * @returns {*|promise}
   * @private
   */
  function _loadCountry(countryDumpLoc) {

    var deferred = Q.defer();

    var rs = fs.createReadStream(countryDumpLoc, {encoding: 'utf8'});

    // TO DO : Add a separate destroy function to
    // properly close and release all the resources.
    rs.pipe(new SeparatorChunker({
      separator: '\n',
      flushTail: false
    }))
        .pipe(new pipes.TrimMe())
        .pipe(new pipes.Split(/\t{1,}/))
        .pipe(new pipes.CountryFilter())
        .pipe(new pipes.Batcher(bufferCount))
        .pipe(new pipes.CountryDBBatchWriter())
        .on('finish', function () {
          deferred.resolve('finished loading the movies information.');
        })
        .on('error', function (err) {
          deferred.reject(err);
        });

    return deferred.promise;
  }


  /**
   * _loadRatings: Update the movie information with
   * the ratings.
   *
   * @param ratingsDumpLoc
   * @private
   */
  function _loadRatings(ratingsDumpLoc) {
    throw new Error('function to be implemented.');
  }


  module.exports = {
    dataStore: dataStore
  };
})();