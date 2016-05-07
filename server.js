var fs = require('fs');
var readline = require('readline');

function process_movies_list(inLoc, outLoc) {

  var start_processing = false;
  var writable = fs.createWriteStream(outLoc, {
    flags: 'w',
    defaultEncoding: 'utf8',
    mode: '0o666'
  });

  writable.on('error', function(error) {
    console.log(error);
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

function normalize_movies_line(data) {

  // base: Trim the string field.
  data = data.trim();

  if (data === '') {
    return;
  }

  // level one: Extract the year of the movie
  // by using split function on the text.
  var row = data.split(/\t{1,}/);

  // stop processing in case we have not
  // enough elements in the
  if(row.length < 2){
    console.log("Not enough arguments.");
    return;
  }

  // level two: Remove the ones with no year
  // or a dash (-) in the year string.
  var yearStr = row[1];
  if(yearStr.indexOf('?') != -1 || yearStr.indexOf('-') != -1) {
    return;
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
      return;
    }
  }

  return row;
}

process_movies_list('./resources/movies-extract-even-more.list', './resources/movies-filtered.list');
