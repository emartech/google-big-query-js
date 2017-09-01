'use strict';

const EventEmitter = require('events');
const TableToFile = require('./');
const BigQuery = require('../big-query');


describe('TableToFile', function() {
  const tableName = 'table_name';
  const file = '[file]';
  let table;

  beforeEach('mock big query', function() {
    table = { export: this.sandbox.stub() };
    this.sandbox.stub(BigQuery, 'create').returns(new BigQuery());
    this.sandbox.stub(BigQuery.prototype, 'table').returns(table);
  });


  describe('.create', function() {
    let instance;

    beforeEach(function() {
      instance = TableToFile.create(tableName, file);
    });


    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(TableToFile);
    });


    it('should instantiate a BigQuery client', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });


    it('should instantiate a BigQuery Table object with proper table name', function() {
      expect(BigQuery.prototype.table).to.calledWithExactly('table_name');
    });

  });


  describe('#run', function() {
    const job = new EventEmitter;
    let result;
    let apiResponse;
    let jobMetadata;


    beforeEach(function*() {
      apiResponse = { status: { state: 'IN_PROGRESS' } };
      jobMetadata = { status: { state: 'DONE' } };
      table.export.resolves([job, apiResponse]);
      setTimeout(() => job.emit('complete', jobMetadata));

      result = yield TableToFile.create(tableName, file).run();
    });


    it('should pass the proper options to export', function() {
      expect(table.export).to.calledWithExactly('[file]', { format: 'JSON', gzip: true });
    });


    it('should propagate error if starting the export job fails', function*() {
      table.export.rejects(new Error('export creation failed'));

      try {
        yield TableToFile.create().run();
      } catch (e) {
        expect(e.message).to.eql('export creation failed');
        return;
      }

      throw new Error('should propagate error');
    });


    it("should return with the query job's result", function() {
      expect(result).to.eql(jobMetadata);
    });


    it('should propagate error of the export job', function*() {
      try {
        setTimeout(() => job.emit('error', new Error('export job failed')));
        yield TableToFile.create().run();
      } catch (e) {
        expect(e.message).to.eql('export job failed');
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
        setTimeout(() => job.emit('complete', 'job-result-object'));
        yield TableToFile.create().run();
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

  });

});
