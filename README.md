# Openscihub Cassandra

This package is basically a wrapper around the Node.JS
[cassandra-driver](https://github.com/datastax/nodejs-driver), which does the
heavy lifting with the CQL protocol. Its purpose is to help build CQL query
strings based on config objects.

## Usage

```js
var merge = require('xtend/immutable');
var CassandraTable = require('osh-cassandra').Table;
var CassandraClient = require('cassandra-driver').Client;

// First define your POJO model using the POD scheme. Add a
// "storage" attribute to each POD to tell the CassandraTable
// mixin how and if it should store the thing.

var UserModel = {
  attrs: {
    username: {
      validate: function(username) {
        if (!/^[a-z]+$/.test(username)) {
          return 'Must be a-z';
        }
      },
      storage: {
        cassandra: 'ascii',
        pg: 'varchar(32)'
      }
    }
  },
  key: ['username']
};

// Now imbue it with Cassandra creation, storage, and retrieval.
var UserTable = merge(UserModel, CassandraTable);

UserTable.client = new CassandraClient({ /* driver options */ });
UserTable.create(function(err) {});
UserTable.drop(function(err) {});
UserTable.insert({ /* opts */ }, function(err) {});
UserTable.select({ /* opts */ }, function(err) {});
```

## Documentation

### Table

#### create(callback)

#### drop(callback)

#### select(opts, callback)

- `opts` **Object, required**
  - `opts.where` **String, required**
  - `opts.consistency`
- `callback` **Function(err, results), required**


## Testing

First, follow the instructions here to start a Cassandra instance in
VirtualBox:

- http://planetcassandra.org/install-cassandra-ova-on-virtualbox/

