(function () {
  // main database file script.
  var pg = require('pg');
  var named = require('node-postgres-named');
  var Q = require('q');
  var connectionString = "postgres://postgres:postgres@localhost/mymdb";

  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name:"movieDB"});


  var movieSchema = [
    "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",

    "CREATE TABLE IF NOT EXISTS movies(" +
    "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), " +
    "title TEXT NOT NULL, " +
    "year INTEGER," +
    "country TEXT," +
    "ratings DECIMAL," +
    "create_time TIMESTAMPTZ," +
    "update_time TIMESTAMPTZ DEFAULT now()" +
    ")",

    "CREATE UNIQUE INDEX movies_title_year_unique ON movies(" +
    "title, " +
    "year" +
    ")",

    "CREATE INDEX idx_movies_country ON movies(" +
    "country" +
    ")",

    "CREATE INDEX idx_movies_ratings ON movies(" +
    "ratings" +
    ")",

    "CREATE INDEX idx_movies_years ON movies(" +
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


  /**
   * FIX ME : Move it to separate file info.
   *
   * addCountries : Update the movie information
   * with corresponding country information.
   *
   * @param countriesInfo
   */
  function addCountries(countriesInfo) {

    var deferred = Q.defer();
    var responses = 0;

    var statement = 'UPDATE movies SET country = $1, update_time = now() WHERE title = $2';

    // TODO : extract rollback in a generic function.
    var rollback = function(client, done) {

      logger.error('an exception occurred, ' +
          'will be rolling back the transaction.');

      client.query('ROLLBACK', function(err){
        return done(err);
      });
    };


    pg.connect(connectionString, function(err, client, done) {

      if (err) {
        logger.error({err:err},'unable to create connection to the database.');
        return deferred.reject(err);
      }

      logger.debug('started updating movies with countries information.');

      client.query('BEGIN', function(err){
        if(err) return rollback(client, done);
      });

      // for each country info, call the
      // update statement.
      countriesInfo.forEach(function(info){
        client.query(statement, [info.country, info.title], function(err) {

          if(err) {
            logger.error(err);

            // reject the promise and rollback the
            // rollback the transaction.
            deferred.reject(err);
            rollback(client, done);
          }

          responses +=1;
          if(responses == countriesInfo.length){
            client.query('COMMIT', done);
            return deferred.resolve('finished processing the batch.');
          }

        })
      })
    });

    return deferred.promise;
  }


  function addRatings(ratingsInfo){

    var deferred = Q.defer();

    var statement = 'UPDATE movies SET ratings = $1, update_time = now() WHERE title = $2';
    var responses = 0;

    function rollback(client, done) {
      log.error('unable to update the ratings information, rolling back');
      client.query('ROLLBACK', done);
    }


    pg.connect(connectionString, function(err, client, done){

      if (err) {
        log.error({error: err}, 'unable to create connection to database to update ratings');
        deferred.reject(err);
        return
      }

      // initiate the batch update in the system
      client.query('BEGIN', function (err){
        if (err) return rollback(client, done);
      });

      ratingsInfo.forEach(function(rating) {
        client.query(statement, [rating.rating, rating.title], function(err){

          if(err) {

            // log the error
            // and reject the promise.
            logger.error(err);
            deferred.reject(err);

            // in addition to this, rollback the transaction.
            rollback(client, done);
            return
          }

          // capture successful responses to be
          // which are helpful in determining whe the promise resolution
          // needs to take place.
          responses ++;
          if (responses  == ratingsInfo.length){
            client.query('COMMIT', done);
            deferred.resolve('ratings information updated');
          }
        })
      })

    });

    return deferred.promise;
  }

  function moviesFilter(filter) {

    logger.info('start fetching the movies based on filter.');

    var deferred = Q.defer();
    var rows = [];

    var q = 'SELECT * FROM movies WHERE 1=1 ';

    if(filter.title !== undefined) {
      q += ' AND title like %$title% '
    }

    if(filter.year !== undefined) {
      q += ' AND year = $year '
    }

    if(filter.country !== undefined) {
      q += ' AND country = $country '
    }

    if(filter.min_rating !== undefined){
      q += ' AND ratings >= $min_rating '
    }

    pg.connect(connectionString, function(err, client, done){

      if(err){
        logger.error('unable to get filtered movies');
        deferred.reject(err);
        return;
      }

      named.patch(client);

      var query = client.query(q, {title: filter.title, country: filter.country, year: filter.year, min_rating: filter.min_rating});

      query.on('row', function(row){
        rows.push(row);
      });

      query.on('error', function(err){
        return deferred.reject(err);
      });

      query.on('end', function(result){
        console.log(result.rowCount);
        done();
        return deferred.resolve(rows);
      });

    });

    return deferred.promise;
  }

  // export the main db interface
  // to be used by the front application.
  module.exports = {
    movieCount: movieCount,
    addMovies: addMovies,
    updateMoviesWithCountryInfo: addCountries,
    updateMoviesWithRatingsInfo: addRatings,
    moviesFilter : moviesFilter,
    init: init
  };
})();
