(function() {

  var fs = require('fs');
  var pipes = require('./pipes');
  var stream = require('stream');
  var readline = require('readline');
  var util = require('util');
  var SeparatorChunker = require('chunking-streams').SeparatorChunker;


  var rs = fs.createReadStream('../resources/movies-extract.list',
     {encoding: 'utf-8'});

  /**
  var Readable = stream.Readable;

  function MyReadable(){
    Readable.call(this, {objectMode: true});
  }

  util.inherits(MyReadable, Readable);

  MyReadable.prototype._read = function(){
    this.push({name: "Abhi"});
  };

  var rs1 = new MyReadable();

  rs1.pipe(pipes.BasicWrite);

  setInterval(function(){
    rs1._read();
  }, 2000);
  **/

  rs.pipe(new SeparatorChunker({
        separator : '\n',
        flushTail : false
      }))
      .pipe(pipes.Trimmer)
      .pipe(pipes.PatternSplit)
      .pipe(pipes.BasicWrite);
})();