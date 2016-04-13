var fs = require('fs');
var readline = require('readline');

function process_movies_list(inLoc, outLoc) {

  var start_processing = false;
  var writable = fs.createWriteStream(outLoc);

  var linereader = readline.createInterface({
    input: fs.createReadStream(inLoc)
  });

  linereader.on('line', function(line) {
    if (start_processing) {
      // read the normalized data from the service
      // and dump the data in the output file.
      data = normalize_movies_line(line);
      if (data !== null && data !== undefined) {
        writable.write(data[0] + '|' + data[1] + '\n');
      }
    }
    if (line.indexOf('========') >= 0) {
      console.log('Start processing the records');
      start_processing = true;
    }
  });

  linereader.on('close', function() {
    console.log('closing the stream');
  });

}


function smallTest(inLoc, outLoc) {

  var reader = readline.createInterface({
    input: fs.createReadStream(inLoc),
    output: fs.createWriteStream(outLoc)
  });

  reader.on('line', function(line) {
    reader.write(line);
  });
}


function normalize_movies_line(data) {
  // STAGE 1 : Trim the string field.
  data = data.trim();

  if (data === '') {
    return;
  }

  console.log(data);
  // Level One : Extract the year of the movie by using
  // split function on the text.
  pieces = data.split(/\t{1,}/);
  return pieces;
}

process_movies_list('./resources/movies-extract.list', './resources/movies-filtered.list');
