(function(){

  var pq = require('pq');
  var Q = require('q');
  var connectionString = "postgres://postgres:postgres@localhost/mymdb";
  var bunyan = require('bunyan');
  var logger = bunyan.createLogger({name:"countryDB"});

  var countrySchema = [

      "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"",

      "CREATE TABLE IF NOT EXISTS country(" +
      "id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), " +
      "country TEXT" +
      "movie_id UUID REFERENCES movies (id)",

      "CREATE INDEX idx_country_country ON country(" +
      "country" +
      ")"
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

      var length = countrySchema.length;
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



  function addCountriesInfo(countryInfo) {
  }



  module.exports = {
    init : init,

  }



})();