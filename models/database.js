(function () {
  // main database file script.
  var pg = require('pg');
  var Q = require('q');
  var connectionString = "postgres://postgres:postgres@localhost/mymdb";

  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name:"db"});


  var movieSchema = [
    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",

    "CREATE TABLE IF NOT EXISTS movies(" +
    "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), " +
    "title TEXT NOT NULL, " +
    "year INTEGER," +
    "create_time TIMESTAMPTZ," +
    "update_time TIMESTAMPTZ DEFAULT now()" +
    ")",

    "CREATE UNIQUE INDEX movies_title_year_unique ON movies(" +
    "title, " +
    "year" +
    ")",

    "CREATE INDEX idx_years ON movies(" +
    "year" +
    ")",

    "CREATE UNIQUE INDEX movies_title_unique ON movies(" +
    "title" +
    ")" +
    "WHERE year IS NULL"
  ];

  /**
   * Initialize the database schema in
   * the system.
   * @returns {*|promise}
   */
  function init() {

    logger.debug('received a request to initialize the database.');
    var deferred = Q.defer();

    pg.connect(connectionString, function (err, client, done) {

      if (err) {
        deferred.reject(err);
        logger.log({err: err},'unable to connect to the database');
        return;
      }

      var length = movieSchema.length;
      var callbackResponses = 0;
      var error = null;

      for (var i = 0; i < length; i++) {
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
      function _statementResponse(err, result) {

        done(); // TODO : Analyze more about done

        if (err) {
          if (err.toString().indexOf('already exists') == -1) {
            error = err;
            logger.error({err : err},'unable to successfully execute statement');
          }
        }

        // capture the number of callbacks.
        callbackResponses++;

        // after all the callbacks, respond with
        // the reject or resolve of the error.
        if (callbackResponses == length) {
          if (error) {
            return deferred.reject(error);
          }
          return deferred.resolve('schema initialized successfully');
        }
      }

    });

    return deferred.promise;
  }

  /**
   * Add a batch of movies in the system.
   * At this moment, we are using prepared statement
   * to improve the execution efficiency.
   *
   * @param movies
   * @returns {*|promise}
   */
  function addMovies(movies) {

    var deferred = Q.defer();
    var statement = 'INSERT INTO movies(title, year, create_time) VALUES ($1, $2, now())';

    var responses = 0;
    var issue;

    var rollback = function(client, done) {

      logger.error('an exception occurred, ' +
          'will be rolling back the transaction.');

      client.query('ROLLBACK', function(err){
        return done(err);
      });
    };


    pg.connect(connectionString, function(err, client, done){

      if (err) {
        logger.error({err:err},'unable to create connection to the database.');
        return deferred.reject(err);
      }

      logger.debug('going to begin the transaction');

      client.query('BEGIN', function(err){
        if(err) return rollback(client, done);
      });

      // start executing query for each movie
      // in the array.
      movies.forEach(function(movie){

        client.query(statement, [movie.title, movie.year], function(err){

          // blocker preventing for any more
          // callbacks to be executed.
          if(issue) return;

          if(err) {

            if(err.toString().indexOf('unique constraint') == -1) {

              logger.error({err:err.toString()});

              // mark that an issue
              // has occurred which would prevent
              // further callbacks to go forward
              issue = err;

              // rollback the client query
              // and reject the promise of batch query.
              rollback(client, done);
              return deferred.reject(err);
            }
          }

          responses +=1;

          if(responses == movies.length){
            // have received all the responses
            // and all of them are yayy ..!!
            client.query('COMMIT', done);
            return deferred.resolve('batch insert completed');
          }

        })
      });

    });

    return deferred.promise;
  }

  /**
   * movieCount fetches the
   * title count in the system.
   */
  function movieCount() {

    // create a promise to be returned.
    var deferred = Q.defer();
    var statement = 'SELECT COUNT(*) FROM movies';

    pg.connect(connectionString, function (err, client, done) {

      // fetch the movie count.
      client.query(statement, handleCountResult);

      function handleCountResult(err, result) {
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
  module.exports = {
    movieCount: movieCount,
    addMovies: addMovies,
    init: init
  };
})();
