(function () {
  'use strict';

  var Transform = require('stream').Transform;
  var Writable = require('stream').Writable;
  var inherits = require('util').inherits;
  var db = require('../models/database');

  /**
   * Trimmer : Trims the string present in the
   * stream.
   *
   * @param options
   * @constructor
   */

  function Trimmer(options) {

    if (!options) {
      options = {};
      options.objectMode = true;
    }

    Transform.call(this, options);
  }

  inherits(Trimmer, Transform);

  Trimmer.prototype._transform = function (chunk, enc, done) {
    this.push(chunk.toString().trim());
    done();
  };


  /**
   * RegexSplit : Split the string based on the
   * pattern provided.
   *
   * @param pattern
   * @param options
   * @constructor
   */
  function RegexSplit(pattern, options) {

    this.pattern = pattern;

    if (!options) {
      options = {
        decodeStrings: false,
        objectMode: true
      }
    }

    Transform.call(this, options);
  }

  inherits(RegexSplit, Transform);

  RegexSplit.prototype._transform = function (chunk, enc, done) {

    if (!(chunk instanceof String)) {
      chunk = chunk.toString()
    }

    this.push(chunk.split(this.pattern));
    done();
  };


  /**
   * MovieFiltering : Implement the internal logic
   * for extracting legible movie information.
   *
   * @param options
   * @constructor
   */
  function MovieFilter(options){

    if(!options) {
      options = {
        objectMode: true
      }
    }

    Transform.call(this, options);
  }

  inherits(MovieFilter, Transform);

  /**
   * _transform : special movie filtering of legible movie
   * information from the
   *
   * @param row
   * @param enc
   * @param done
   * @private
   */
  MovieFilter.prototype._transform = function(row, enc, done){

    if(row.length < 2){
      return done();
    }

    var yearStr = row[1];
    if(yearStr.indexOf('?') != -1 || yearStr.indexOf('-') != -1) {
      row[1] = null;
    }

    // level three: Remove the ones wth sitcom
    // episode.
    var episodeStr = row[0];
    var sitcom = episodeStr.match(/\{(.*)\}/i); // {episode #1} <- remove it.

    if(sitcom) {
      // need to check for the episode.
      // strings [{#1.4}, {1992-10-01}] should be avoided.
      var extract = sitcom[1];
      if (extract.indexOf('#') != -1 || extract.indexOf('-') != -1) {
        return done();
      }
    }
    done(null, {title: row[0], year: row[1]});
  };

  /**
   * Batcher: Based on the size supplied,
   * it collects and batches the information supplied to
   * from the base stream.
   *
   * @param batchSize
   * @param options
   * @constructor
   */
  function Batcher(batchSize, options) {

    this.buffer =[];
    this.batchSize = batchSize;

    if(!options){
      options = {
        objectMode : true
      }
    }
    Transform.call(this, options);
  }

  inherits(Batcher, Transform);

  Batcher.prototype._transform = function(row, enc, done) {

    this.buffer.push(row);

    if(this.buffer.length >= this.batchSize) {
      this.push(this.buffer);
      this.buffer = [];
    }

    done();
  };


  /**
   * DBWriter : Writes a batch of movie entries in the system.
   * @param options
   * @constructor
   */
  function MovieDbBatchWriter(options) {

    if(!options){
      options = {
        objectMode: true
      }
    }

    Writable.call(this, options);
  }

  inherits(MovieDbBatchWriter, Writable);

  MovieDbBatchWriter.prototype._write = function(batch, enc, done) {
    // write to the database here.

    db.addMovies(batch)
        .then(function(){
          done();
        })
        .catch(function(err){
          done(err);
        });
  };

  module.exports = {
    TrimMe: Trimmer,
    Split: RegexSplit,
    MovieFilter: MovieFilter,
    Batcher: Batcher,
    MovieDbBatchWriter : MovieDbBatchWriter
  }

})();