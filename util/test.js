(function() {

  var fs = require('fs');
  var pipes = require('./pipes');
  var stream = require('stream');
  var readline = require('readline');
  var util = require('util');
  var SeparatorChunker = require('chunking-streams').SeparatorChunker;


  var rs = fs.createReadStream('../resources/genres-copy.list',
     {encoding: 'utf-8'});

  rs.on('close', function(){
    console.log('I am closed now ... $$$$$$');
  });

  var writer = rs.pipe(new SeparatorChunker({
        separator : '\n',
        flushTail : false
      }))
      .pipe(new pipes.TrimMe())
      .pipe(new pipes.Split(/\t{1,}/))
      .pipe(new pipes.GenreFilter())
      .pipe(new pipes.Batcher(5000))
      .pipe(new pipes.GenreDBBatchWriter())
      .on('error', function(err){
        rs.destroy();
      });

  writer.on('finish', function(){
   console.log('finished loading the information finally');
  });

})();