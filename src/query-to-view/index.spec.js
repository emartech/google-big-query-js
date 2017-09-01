'use strict';

const QueryToView = require('./');
const BigQuery = require('../big-query');


describe('QueryToView', function() {

  beforeEach('mock big query', function() {
    this.sandbox.stub(BigQuery, 'create').returns(new BigQuery());
  });


  describe('.create', function() {
    let instance;

    beforeEach(function() {
      instance = QueryToView.create();
    });


    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(QueryToView);
    });


    it('should instantiate a BigQuery client', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });

  });


  describe('#run', function() {
    let table = '[TABLE]';

    beforeEach(function() {
      this.sandbox.stub(BigQuery.prototype, 'createTable').resolves(table);
    });


    context('when called with an SQL query', function() {
      let result;

      beforeEach(function*() {
        result = yield QueryToView.create('destination_table').run('sql-query-to-run');
      });


      it('should create a table with the proper table name and options', function() {
        expect(BigQuery.prototype.createTable).to.calledWithExactly('destination_table', {
          id: 'destination_table',
          kind: 'bigquery#table',
          tableReference: {
            projectId: 'main_project',
            datasetId: 'main_dataset',
            tableId: 'destination_table'
          },
          view: {
            query: 'sql-query-to-run',
            useLegacySql: false
          }
        });
      });


      it('should return with the created table object', function() {
        expect(result).to.eql('[TABLE]');
      });

    });

  });

});
