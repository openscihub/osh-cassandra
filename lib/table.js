var blanks = require('./blanks');
var extend = require('xtend/mutable');
var cassandra = require('cassandra-driver');

/**
 *  Make building simple queries a little easier.  We can save some typing by
 *  storing the column names and types of a table in a POJO.
 */

function Table(spec) {
  if (!(this instanceof Table)) {
    return new Table(spec);
  }

  this.spec = spec;
  this.name = spec.name;
  this.attrs = spec.attrs;
  this.attrNames = Object.keys(spec.attrs);
  this.primaryKey = spec.primaryKey;
  this.postfix = spec.postfix;
  //this.filter = Filter(this.attrNames);
  this.defaultConsistency = spec.consistency || cassandra.types.consistencies.quorum;
  this.blanks = blanks(this.attrNames.length);
  this._client = spec.client;
}

Table.prototype = {
  constructor: Table,

  /**
   *  Set or get CQL client.
   */

  client: function(client) {
    if (client) this._client = client;
    else if (!this._client) {
      throw new Error('No client attached to Cassandra table ' + this.name);
    }
    else return this._client;
  },

  createCQL: function() {
    if (!this._createCQL) {
      var table = this;
      this._createCQL = (
        'CREATE TABLE IF NOT EXISTS ' + this.name + ' (' +
        this.attrNames.map(function(attrName) {
          return attrName + ' ' + table.attrs[attrName];
        }).join(', ') + ', ' +
        'PRIMARY KEY (' + this.primaryKey.map(function(part, index) {
          if (typeof part == 'object') {
            return '(' + part.join(', ') + ')';
          }
          else {
            return part;
          }
        }).join(', ') + ')' +
        ')' + (this.postfix || '')
      );
    }
    return this._createCQL;
  },

  insertCQL: function() {
    if (!this._insertCQL) {
      this._insertCQL = (
        'INSERT INTO ' + this.name + ' (' +
          this.attrNames.join(', ') +
        ') VALUES ' + this.blanks
      );
    }
    return this._insertCQL;
  },

  //updateCQL: function() {
  //  if (!this._updateCQL) {
  //    this._updateCQL = (
  //      'UPDATE ' + this.name + ' SET'

  drop: function(done) {
    this.client().execute('DROP TABLE IF EXISTS ' + this.name, done);
  },

  create: function(done) {
    var name = this.name;
    this.client().execute(this.createCQL(), function(err, result) {
      done(
        err ? 'Error creating table ' +
        name + ': ' + err : null
      );
    });
  },

  insert: function(record, options, done) {
    var spec = this.spec;

    if (!done) {
      done = options;
      options = {};
    }

    var overwrite = options.overwrite;
    var client = this.client();
    //var execute = this._getExecute(options);

    // Must supply all table attributes.

    var attrName, i, len;
    var values = [];
    for (i = 0, len = this.attrNames.length; i < len; i++) {
      attrName = this.attrNames[i];
      if (attrName in record) {
        values.push(record[attrName]);
      }
      else {
        var tableName = this.name;
        return process.nextTick(function() {
          done(new Error(
            'Insert ' + tableName + ': Must supply ' + attrName
          ));
        });
      }
    }

    client.execute(
      this.insertCQL() + (overwrite ? '' : ' IF NOT EXISTS'),
      values,
      //this.attrNames.map(
      //  function(attrName) {return record[attrName]}
      //),
      extend(
        {
          consistency: this.defaultConsistency
        },
        options
      ),
      function(err, result) {
        console.log(result);
        if (err) done(err);
        else if (!overwrite && !result.rows[0]['[applied]']) {
          done(null, false);
        }
        else {
          done(null, true);
        }
      }
    );
  },

  update: function(record, options, done) {
    if (!done) {
      done = options;
      options = {};
    }
    options = options || {};

    var client = this.client();
    //var execute = this._getExecute(options);

    var values = [];
    var CQL = (
      'UPDATE ' + this.name + ' SET ' +
      Object.keys(record).map(function(name) {
        values.push(record[name]);
        return name + '=?';
      }).join(', ') +
      (options.where ? ' WHERE ' + options.where : '') +
      ';'
    );

    Array.prototype.push.apply(values, options.values);

    client.execute(
      CQL,
      values,
      extend(
        {
          consistency: this.defaultConsistency
        },
        options
      ),
      done
    );
  },

  _getExecute: function(opts) {
    var client = this.client();
    return (
      opts.prepare ?
      client.executeAsPrepared.bind(client) :
      client.execute.bind(client)
    );
  },

  /**
   *  Return table attribute names that are not explicitly excluded
   *  by the `exclude` option.
   */

  filterAttrs: function(opts) {
    var exclude = opts.exclude || [];
    var filteredAttrs = [];
    this.attrNames.forEach(function(attrName) {
      exclude.indexOf(attrName) < 0 && filteredAttrs.push(attrName);
    });
    return filteredAttrs;
  },


  select: function(opts, callback) {
    var self = this;
    var name = self.name;
    var attrNames = self.filterAttrs(opts);
    var rowCallback;
    var client = this.client();

    if (opts.rowCallback) {
      rowCallback = function(i, row) {
        delete row.columns;
        opts.rowCallback(i, row);
      };
    }

    var args = [
      (
        'SELECT ' + attrNames.join(', ') + ' ' +
        'FROM ' + name + ' ' +
        'WHERE ' + opts.where
      ),
      opts.values,
      opts.consistency || this.defaultConsistency,
      rowCallback || endCallback
    ];

    if (rowCallback) {
      args = args.concat(endCallback);
      client.eachRow.apply(client, args);
    }
    else {
      self._getExecute(opts).apply(null, args);
    }

    function endCallback(err, result) {
      if (err) callback(err);
      else {
        if (opts.one) {
          var count = rowCallback ? result : result.rows.length;
          if (count > 1) {
            return callback(new Error(
              'SELECT on ' + name + ' expected a single row. ' +
              'Got ' + count + '.'
            ));
          }
        }

        callback(null, rowCallback ? result : (
          opts.one ? result.rows[0] : result.rows
        ));
      }
    }
  }
};

module.exports = Table;
