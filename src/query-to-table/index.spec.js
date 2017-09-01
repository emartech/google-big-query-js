'use strict';

const EventEmitter = require('events');
const QueryToTable = require('./');
const BigQuery = require('../big-query');


describe('QueryToTable', function() {

  beforeEach('mock big query', function() {
    this.sandbox.stub(BigQuery, 'create').returns(new BigQuery());
  });


  describe('.create', function() {
    let instance;

    beforeEach(function() {
      instance = QueryToTable.create();
    });


    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(QueryToTable);
    });


    it('should instantiate a BigQuery client', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });

  });


  describe('#run', function() {
    const job = new EventEmitter;
    let apiResponse = {};
    let jobMetadata = {};


    beforeEach(function() {
      jobMetadata.status = { state: 'DONE' };
      apiResponse.status = { state: 'IN_PROGRESS' };

      this.sandbox.stub(BigQuery.prototype, 'startQuery').resolves([job, apiResponse]);
    });


    context('when called with an SQL query', function() {
      let result;

      beforeEach(function*() {
        setTimeout(() => job.emit('complete', jobMetadata));
        result = yield QueryToTable.create('destination_table').run('sql-query-to-run');
      });


      it('should pass the proper query object', function() {
        expect(BigQuery.prototype.startQuery).to.calledWithExactly({
          query: 'sql-query-to-run',
          useLegacySql: false,
          destinationTable: {
            projectId: 'main_project',
            datasetId: 'main_dataset',
            tableId: 'destination_table'
          },
          writeDisposition: 'WRITE_TRUNCATE',
          maximumBillingTier: 100
        });
      });


      it('should propagate error if starting the query job fails', function*() {
        BigQuery.prototype.startQuery.rejects(new Error('error in startQuery'));

        try {
          yield QueryToTable.create().run();
        } catch (e) {
          expect(e.message).to.eql('error in startQuery');
          return;
        }

        throw new Error('should propagate error');
      });


      it("should return with the query job's result", function() {
        expect(result).to.eql({ status: { state: 'DONE' } });
      });


      it('should propagate error of the query job', function*() {
        try {
          setTimeout(() => job.emit('error', new Error('error-from-job')));
          yield QueryToTable.create().run();
        } catch (e) {
          expect(e.message).to.eql('error-from-job');
          return;
        }

        throw new Error('should propagate error');
      });


      it('should throw error if the query contains errors', function*() {
        apiResponse.status.errorResult = {
          reason: 'invalid',
          location: 'quantile_limits',
          message: 'Table name cannot be resolved: dataset name is missing.'
        };

        try {
          setTimeout(() => job.emit('complete', jobMetadata));
          yield QueryToTable.create().run();
        } catch (error) {
          expect(error.message).to.eql(JSON.stringify({
            reason: 'invalid',
            location: 'quantile_limits',
            message: 'Table name cannot be resolved: dataset name is missing.'
          }));
          return;
        }

        throw new Error('should propagate error');
      });


      it('should throw error if the job cannot be executed', function*() {
        jobMetadata.status.errorResult = { reason: 'billingTierLimitExceeded' };

        try {
          setTimeout(() => job.emit('complete', jobMetadata));
          yield QueryToTable.create().run();
        } catch (error) {
          expect(error.message).to.eql(JSON.stringify({ reason: 'billingTierLimitExceeded' }));
          return;
        }

        throw new Error('should propagate error');

      });

    });


    context('when additional BigQuery parameters are passed to the query', function() {

      it('should pass the additional parameters to the job', function*() {
        setTimeout(() => job.emit('complete', jobMetadata));
        yield QueryToTable.create('destination_table')
          .run('other-sql-query-to-run', {
            useLegacySql: 'overwritten',
            newField: 'new value'
          });

        const passedParameters = BigQuery.prototype.startQuery.firstCall.args[0];
        expect(passedParameters).to.containSubset({
          useLegacySql: 'overwritten',
          newField: 'new value'
        });
      });

    });

  });

});
