var cassandra = require('cassandra-driver');
var Client = cassandra.Client;
var Table = require('..').Table;
var async = require('async');
var expect = require('expect.js');

describe('osh-cassandra', function() {
  var client;
  var host = 'localhost'; //'127.0.0.1:9042';

  before(function(done) {
    client = new Client({contactPoints: [host]});
    client.execute(
      'CREATE KEYSPACE IF NOT EXISTS test_osh_cassandra ' +
        'WITH REPLICATION = {' +
          '\'class\': \'SimpleStrategy\',' +
          '\'replication_factor\': 1' +
          //'"class": "NetworkTopologyStrategy"' +
          //'"datacenter1": 2' +
        '};',
      [],
      function(err, result) {
        if (err) done(err);
        else client.shutdown(done);
      }
    );
  });

  before(function() {
    client = new Client({
      contactPoints: [host],
      keyspace: 'test_osh_cassandra'
    });
  });

  describe('Table', function() {
    describe('create()/drop()', function() {
      it('should work', function(done) {
        var TestTable = new Table({
          name: 'test_table',
          attrs: {
            id: 'uuid'
          },
          primaryKey: ['id']
        });
        TestTable.client(client);
        TestTable.create(function(err) {
          if (err) return done(err);
          TestTable.drop(done);
        });
      });
    });

    describe('insert()', function() {
      var TestTable;

      before(function(done) {
        TestTable = new Table({
          name: 'test_table',
          attrs: {
            id: 'int'
          },
          primaryKey: ['id'],
          consistency: cassandra.types.consistencies.one
        });
        TestTable.client(client);
        TestTable.drop(done);
      });

      it('should work', function(done) {
        function insert(done) {
          TestTable.insert({id: 1}, {prepare: true}, function(err, status) {
            if (err) return done(err);
            expect(status).to.be.ok();
            done();
          });
        }

        function check(done) {
          client.execute(
            'SELECT id FROM test_table WHERE id = ?',
            [1],
            {
              consistency: cassandra.types.consistencies.one,
              prepare: true
            },
            function(err, result) {
              if (err) done(err);
              else {
                expect(result.rows[0].id).to.be(1);
                done();
              }
            }
          );
        }

        var tasks = [
          TestTable.create.bind(TestTable),
          insert,
          check,
          TestTable.drop.bind(TestTable)
        ];

        async.series(tasks, done);
      });
    });
  });
});
