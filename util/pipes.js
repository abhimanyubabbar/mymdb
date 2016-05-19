(function () {
  'use strict';

  var Transform = require('stream').Transform;
  var Writable = require('stream').Writable;
  var inherits = require('util').inherits;
  var db = require('../models/database');
  var util = require('util').inherits;

  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name: 'pipes'});

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

    if (Buffer.isBuffer(chunk)) {
      // convert it to string and then push.
      this.push(chunk.toString().trim());
    } else if (chunk instanceof Object) {
      var key;
      for (key in chunk) {
        if (chunk.hasOwnProperty(key)) {

          // trim the string key values of the chunk.
          if (chunk[key] instanceof String) {
            chunk[key] = chunk[key].trim();
          }
        }
      }
      this.push(chunk);
    } else {
      // just push the trimmed chunk.
      this.push(chunk.trim());
    }
    // finally inform the upstream about the same.
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

    if (!(typeof chunk === 'string')) {
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
  function MovieFilter(options) {

    if (!options) {
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
  MovieFilter.prototype._transform = function (row, enc, done) {

    if (row.length < 2) {
      return done();
    }

    var yearStr = row[1];
    if (yearStr.indexOf('?') != -1 || yearStr.indexOf('-') != -1) {
      row[1] = null;
    }

    // level three: Remove the ones wth sitcom
    // episode.
    var episodeStr = row[0];
    var sitcom = episodeStr.match(/\{(.*)\}/i); // {episode #1} <- remove it.

    if (sitcom) {
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

    this.buffer = [];
    this.batchSize = batchSize;

    if (!options) {
      options = {
        objectMode: true
      }
    }
    Transform.call(this, options);
  }

  inherits(Batcher, Transform);

  Batcher.prototype._transform = function (row, enc, done) {

    this.buffer.push(row);

    if (this.buffer.length >= this.batchSize) {
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

    if (!options) {
      options = {
        objectMode: true
      }
    }

    Writable.call(this, options);
  }

  inherits(MovieDbBatchWriter, Writable);

  MovieDbBatchWriter.prototype._write = function (batch, enc, done) {
    // write to the database here.

    db.addMovies(batch)
        .then(function () {
          done();
        })
        .catch(function (err) {
          done(err);
        });
  };


  /**
   * CountryFilter : The filter applied for determining the country
   * of the movie.
   *
   * @param options
   * @constructor
   */
  function CountryFilter(options){

    if (!options) {
      options = {
        objectMode : true
      };
    }

    Transform.call(this, options);
  }

  inherits(CountryFilter, Transform);

  /**
   * Apply the transformation for the filtering the rows with the countries.
   * @param row
   * @param enc
   * @param done
   * @returns {*}
   * @private
   */
  CountryFilter.prototype._transform = function(row, enc, done){

    if (row.length < 2) {
      return done();
    }

    var episodeStr = row[0];
    var sitcom = episodeStr.match(/\{(.*)\}/i); // {episode #1} <- remove it.

    if (sitcom) {
      // need to check for the episode.
      // strings [{#1.4}, {1992-10-01}] should be avoided.
      var extract = sitcom[1];
      if (extract.indexOf('#') != -1 || extract.indexOf('-') != -1) {
        return done();
      }
    }
    done(null, {title: row[0].trim(), country: row[1].trim()});
  };
  /**
   * CountryDBBatchWriter : Writer which executes the
   * updates to the db and adds country information to movies.
   *
   * @param options
   * @constructor
   */
  function CountryDBBatchWriter(options) {

    if(!options) {
      options = {
        objectMode : true
      };
    }

    Writable.call(this, options);
  }

  inherits(CountryDBBatchWriter, Writable);

  /**
   * Sink which updates the movies with the countries
   * information.
   *
   * @param rows
   * @param enc
   * @param done
   * @private
   */
  CountryDBBatchWriter.prototype._write = function(rows, enc, done) {

    db.updateMoviesWithCountryInfo(rows)
        .then(function(){
          done();
        })
        .catch(function(err){
          logger.error({err: err}, 'unable to update a countries batch in system.');
          done(err);
        })
  };


  /**
   * RatingsDBBatchWriter : The writer for pushing the
   * batched information in the database.
   *
   * @param options
   * @constructor
   */
  function RatingsDBBatchWriter(options) {

    if(!options) {
      options = {
        objectMode : true
      }
    }

    Writable.call(this, options);
  }

  inherits(RatingsDBBatchWriter, Writable);

  /**
   * _write: perform the main db push.
   * @param rows
   * @param enc
   * @param done
   * @private
   */
  RatingsDBBatchWriter.prototype._write = function(rows, enc , done) {

    db.updateMoviesWithRatingsInfo(rows)
        .then(function(){
          done();
        })
        .catch(function(err){
          logger.error({err: err}, 'unable to update a ratings batch in system.');
          done(err);
        })

  };


  /**
   * RatingsFilter : Filter the rows with valid ratings
   * information and push the valid object to the listening
   * pipe.
   * @param options
   * @constructor
   */
  function RatingsFilter(options){

    if(!options) {
      options = {
        objectMode : true
      }
    }

    Transform.call(this, options);
  }

  inherits(RatingsFilter, Transform);

  /**
   * _transform : Perform the main ratings transformation in
   * the system.
   *
   * @param chunk
   * @param enc
   * @param done
   * @private
   */
  RatingsFilter.prototype._transform = function(chunk, enc, done) {

    if(!(typeof chunk == 'string')) {
      chunk = chunk.toString()
    }

    var baseSplit = chunk.split(/\ +/, 3);

    if(baseSplit.length != 3) {
      return
    }

    if (isNaN(baseSplit[0]) || isNaN(baseSplit[1])) {
      done();
      return
    }

    var start = chunk.indexOf(baseSplit[2]);
    var subStr = chunk.substr(start)



  };

  module.exports = {
    TrimMe: Trimmer,
    Split: RegexSplit,
    MovieFilter: MovieFilter,
    Batcher: Batcher,
    MovieDbBatchWriter: MovieDbBatchWriter,
    CountryDBBatchWriter: CountryDBBatchWriter,
    CountryFilter: CountryFilter,
    RatingsDBBatchWriter : RatingsDBBatchWriter,
    RatingsFilter : RatingsFilter
  }

})();