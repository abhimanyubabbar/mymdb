var fs = require('fs');
var readline = require('readline');

function process_movies_list(loc) {

  var start_processing = false;
  var linereader = readline.createInterface({
    input: fs.createReadStream(loc)
  });

  linereader.on('line', function(line) {
    if (start_processing) {
      normalize_movies_line(line);
    }
    if (line.indexOf('========') >= 0) {
      console.log('Start processing the records');
      start_processing = true;
    }
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

  // NORMALIZE : piece[0] -> movie_name
  console.log('year: ' + pieces[1]);
}

process_movies_list('./resources/movies-extract.list');
