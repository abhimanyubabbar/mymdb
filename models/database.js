// main database file script.
var pg = require('pg');
var Q = require('q');
var connectionString = "postgres://postgres:postgres@localhost/mymdb";
var error = null;


var movieSchema = [
    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",

    "CREATE TABLE IF NOT EXISTS movies(" +
    "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), " +
    "title TEXT, " +
    "year INTEGER" +
    ")",

    "CREATE UNIQUE INDEX movie_year_unique ON movies(" +
    "title, " +
    "year" +
    ")"
];

var client = new pg.Client(connectionString);

pg.connect(connectionString, function(err, client, done){
  if(err) {
    error = err;
    return console.log('unable to connect to the database');
  }


  for(var i = 0; i < movieSchema.length; i++) {
    // execute the statements in the movies schema.
    client.query(movieSchema[i], statementResponse);
  }

  /**
   * Generic response handling for the
   * response for statement execution for the
   * schema creation.
   * @param err
   * @param result
   * @returns {*}
   */
  function statementResponse(err, result){
    done();
    if (err) {
      error = err;
      return console.log('unable to successfully execute statement');
    }
    console.log(result);
  }

});

var db = {
  error : error,
  movieCount : movieCount
};


/**
 * movieCount fetches the
 * title count in the system.
 */
function movieCount(){

  // create a promise to be returned.
  var deferred = Q.defer();
  var statement = 'SELECT COUNT(*) FROM movies';

  pg.connect(connectionString, function(err, client, done){

    // fetch the movie count.
    client.query(statement, handleCountResult);

    function handleCountResult(err, result){
      // return the connection to the pool.
      done();

      if (err) {
        deferred.reject(new Error(err));
        return;
      }

      // able to pull in the movie count.
      var obj = result.rows[0];
      deferred.resolve(obj.count);
    }
  });

  return deferred.promise;
}

// export the main db interface
// to be used by the front application.
module.exports = db;
