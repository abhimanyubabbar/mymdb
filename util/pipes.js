(function () {
  'use strict';

  var Transform = require('stream').Transform;
  var Writable = require('stream').Writable;
  var util = require('util');


  /**
   *
   * @param options
   * @returns {Trimmer}
   * @constructor
   */
  function Trimmer(options){
    if(!this instanceof Trimmer){
      return new Trimmer(options);
    }

    if(!options) options={};
    options.decodeStrings = false;

    Transform.call(this, options);
  }

  util.inherits(Trimmer, Transform);

  /**
   *
   * @param line
   * @param enc
   * @param done
   * @private
   */
  Trimmer.prototype._transform = function(line, enc, done){
    this.push(line.toString().trim());
    done();
  };


  /**
   *
   * @param options
   * @returns {Splitter2}
   * @constructor
   */
  function Splitter2(options) {

    if (!this instanceof Splitter2) {
      return new Splitter2(options);
    }

    if (!options) options = {};
    options.decodeStrings= false;

    Transform.call(this, options);
  }

  util.inherits(Splitter2, Transform);

  /**
   *
   * @param line
   * @param enc
   * @param cb
   * @private
   */
  Splitter2.prototype._transform = function (line, enc, cb) {

    var moreFragments = line.toString().split('\n');

    if(moreFragments){
      for(var i=0; i < moreFragments.length; i ++){
        this.push(moreFragments[i]);
      }
      cb();
    } else {
      cb(null, line);
    }
  };

  /**
   *
   * @param pattern
   * @param options
   * @constructor
   */
  function PatternSplit(pattern, options) {

    if (!this instanceof PatternSplit){
      return new PatternSplit(options);
    }

    if(!options) options = {};
    options.objectMode = true;
    options.highWaterMark = true;

    Transform.call(this,pattern, options);
  }

  util.inherits(PatternSplit, Transform);

  /**
   *
   * @param line
   * @param enc
   * @param done
   * @private
   */
  PatternSplit.prototype._transform = function(line, enc, done){

    // This will fail in case writer is stdout which is writing
    // to the process using in strings and therefore you cannot push an object to it.
    var row = line.toString().split(this.pattern);
    console.log({data:row});
    this.push({data : 'abhi'});
    done();
  };

  /**
   *
   * @param options
   * @returns {ObjectWrite}
   * @constructor
   */
  function ObjectWrite(options){

    if (!(this instanceof ObjectWrite)){
      return new ObjectWrite(options);
    }

    if (!options){
      options = {};
      options.objectMode = true;
    }

    Writable.call(this, options);
  }

  util.inherits(ObjectWrite, Writable);

  /**
   *
   * @param data
   * @param enc
   * @param done
   * @private
   */
  ObjectWrite.prototype._write = function(data, enc, done){
    console.log(data);
    done()
  };

  module.exports = {
    Trimmer: new Trimmer(),
    Splitter2: new Splitter2(),
    PatternSplit: new PatternSplit(/\t{1,}/),
    BasicWrite: new ObjectWrite()
  };
})();