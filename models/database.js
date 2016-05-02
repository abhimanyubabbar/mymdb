// main database file script.
var pg = require('pg');
var Q = require('q');
var connectionString = "postgres://postgres:postgres@localhost/mymdb";


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


var db = {
  movieCount : movieCount,
  init : init
};

/**
 * Initialize the database schema in
 * the system.
 * @returns {*|promise}
 */
function init() {

  var deferred = Q.defer();

  pg.connect(connectionString, function(err, client, done){

    if(err) {
      deferred.reject(new Error(err));
      return console.log('unable to connect to the database');
    }

    var length = movieSchema.length;
    var callbackResponses=0;
    var error = null;

    for(var i = 0; i < length; i++) {
      // execute the statements in the movies schema.
      client.query(movieSchema[i], _statementResponse);
    }

    /**
     * Generic response handling for the
     * response for statement execution for the
     * schema creation.
     * @param err
     * @param result
     * @returns {*}
     */
    function _statementResponse(err, result){

      done(); // TODO : Analyze more about done

      if (err) {
        if(err.toString().indexOf('already exists') == -1){
          error = new Error(err);
          console.log('unable to successfully execute statement');
        }
      } else {
        console.log(result);
      }

      // capture the number of callbacks.
      callbackResponses++;

      // after all the callbacks, respond with
      // the reject or resolve of the error.
      if (callbackResponses == length) {
        if (error){
          return deferred.reject(error);
        }
        return deferred.resolve('schema initialized successfully');
      }
    }

  });

  return deferred.promise;
}

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
