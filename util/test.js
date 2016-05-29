(function() {

  var fs = require('fs');
  var pipes = require('./pipes');
  var stream = require('stream');
  var readline = require('readline');
  var util = require('util');
  var SeparatorChunker = require('chunking-streams').SeparatorChunker;


  var rs = fs.createReadStream('../resources/ratings-test.list',
     {encoding: 'utf-8'});

  var writer = rs.pipe(new SeparatorChunker({
        separator : '\n',
        flushTail : false
      }))
      .pipe(new pipes.TrimMe())
      .pipe(new pipes.RatingsFilter())
      .pipe(new pipes.Batcher(5000))
      .on('end', function(){
        console.log('received an end event in the system.' + this.buffer.length);
      })
      .pipe(new pipes.RatingsDBBatchWriter())
      .on('error', function(err){
        rs.destroy();
      });

  writer.on('finish', function(){
   console.log('finished loading the information finally');
  });

  rs.on('close', function(){
    console.log('I am closed now ... $$$$$$');
  });

})();