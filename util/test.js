(function() {

  var fs = require('fs');
  var pipes = require('./pipes');
  var stream = require('stream');
  var readline = require('readline');
  var util = require('util');
  var SeparatorChunker = require('chunking-streams').SeparatorChunker;


  var rs = fs.createReadStream('../resources/movies-copy.list',
     {encoding: 'utf-8'});

  rs.pipe(new SeparatorChunker({
        separator : '\n',
        flushTail : false
      }))
      .pipe(new pipes.TrimMe())
      .pipe(new pipes.Split(/\t{1,}/))
      .pipe(new pipes.MovieFilter())
      .pipe(new pipes.Batcher(10000))
      .pipe(new pipes.MovieDbBatchWriter());
})();