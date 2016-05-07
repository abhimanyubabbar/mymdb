(function(){
  'use strict';

  var fs = require('fs');
  var readline = require('readline');
  var util = require('util');
  var EventEmitter = require('events');

  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name:"file-stream"});

  // My own stream emitter.
  // There doesn't seem to be any explicit purpose
  // to create separate emitter and instead use the
  // base stream emitter.
  function MyReadStreamEmitter() {
    EventEmitter.call(this);
  }

  // create a prototypical link between the two.
  util.inherits(MyReadStreamEmitter, EventEmitter);

  /**
   * Create a read stream which provides
   * object to be used by application.
   *
   * @param location
   * @returns {MyReadStreamEmitter}
   */
  function createObjectReadStream(location){

    var linereader = readline.createInterface({
      input: fs.createReadStream(location, {
        flags: 'r',
        encoding: 'utf8',
        mode:'0o666'
      })
    });

    // API Expose BackPressure.
    MyReadStreamEmitter.prototype.pause = function(){
      logger.info('request to pause the stream.');
      linereader.pause();
    };

    MyReadStreamEmitter.prototype.resume = function(){
      logger.info('request to resume the stream.');
      linereader.resume();
    };

    var emitter = new MyReadStreamEmitter();

    // Basic EventListeners on line reader.
    linereader.on('line', _readLine);

    // error event listener.
    linereader.on('error', function(error){

      logger.error({err: error},'unable to process the raw file stream.');
      linereader.removeListener('line', _readLine);
      // inform the listeners about
      // the same.
      emitter.emit('error', error)
    });

    // close event listener.
    linereader.on('close', function() {
      logger.info('closing the stream');
      emitter.emit('close');
    });

    /**
     * _readLine : captures the data from the downstream
     * and emits the information to the application after
     * filtering.
     *
     * @param line
     * @private
     */
    function _readLine(line){
      var data = _dataFiltering(_normalize(line));
      if (data != null) {
        // emit data to the reading application.
        emitter.emit('data', {title: data[0], year: data[1]});
      }
    }

    return emitter;
  }

  /**
   * Basic data filtering before
   * the information could be forwarded
   * to the application.
   *
   * @param data
   * @returns {*}
   * @private
   */
  function _dataFiltering(data){

    if (data == null) {
      return null;
    }

    var title = data[0];
    var year = data[1];

    if ( year == '') {
      year = null
    } else {
      year = parseInt(year);
    }
    return [title.trim(), year];
  }


  /**
   * Process the data list in the file.
   * This is an internal method which needs to be
   * deprecated in future.
   *
   * @param inLoc
   * @param outLoc
   */
  function processList(inLoc, outLoc) {

    var start_processing = false;
    var writable = fs.createWriteStream(outLoc, {
      flags: 'w',
      defaultEncoding: 'utf8',
      mode: '0o666'
    });

    writable.on('error', function(error) {
      logger.error(error);
    });

    var linereader = readline.createInterface({
      input: fs.createReadStream(inLoc, {
        flags: 'r',
        encoding: 'utf8',
        mode: '0o666'
      })
    });

    linereader.on('line', function(line) {
      if (start_processing) {
        // read the normalized data from the service
        // and dump the data in the output file.
        var data = _normalize(line);
        if (data !== null && data !== undefined) {
          writable.write(data[0] + '|' + data[1] + '\n');
        }
      }
      if (line.indexOf('========') >= 0) {
        logger.debug('Start processing the records');
        start_processing = true;
      }
    });

    linereader.on('close', function() {
      logger.debug('closing the stream');
    });

  }

  /**
   * Normalize the data passed in the system
   * based on the process logic.
   *
   * @param data
   * @returns {Array}
   * @private
   */
  function _normalize(data) {

    // base: Trim the string field.
    data = data.trim();

    if (data === '') {
      return null;
    }

    // level one: Extract the year of the movie
    // by using split function on the text.
    var row = data.split(/\t{1,}/);

    // stop processing in case we have not
    // enough elements in the
    if(row.length < 2){
      logger.trace("Not enough arguments.");
      return null;
    }

    // level two: Remove the ones with no year
    // or a dash (-) in the year string.
    var yearStr = row[1];
    if(yearStr.indexOf('?') != -1 || yearStr.indexOf('-') != -1) {
      row[1] = '';
    }

    // level three: Remove the ones wth sitcom
    // episode.
    var episodeStr = row[0];
    var sitcom = episodeStr.match(/\{(.*)\}/i); // {episode #1} <- remove it.

    if(sitcom !== null) {
      // need to check for the episode.
      // strings [{#1.4}, {1992-10-01}] should be avoided.
      var extract = sitcom[1];
      if (extract.indexOf('#') != -1 || extract.indexOf('-') != -1) {
        return null;
      }
    }
    return row;
  }

  module.exports = {
    processList : processList,
    createObjectReadStream : createObjectReadStream
  };
})();
