'use strict';

const { File } = require('@emartech/google-cloud-storage');
const QueryToFile = require('./');
const QueryToTable = require('../query-to-table');
const TableToFile = require('../table-to-file');
const BigQuery = require('../big-query');


describe('QueryToFile', function() {
  let clock;
  let table;
  let instance;

  beforeEach(function() {
    clock = this.sandbox.useFakeTimers(+new Date('2016-12-08')); //= 1481155200000
    table = { delete: this.sandbox.stub().resolves() };

    instance = QueryToFile.create('base_name');
  });


  afterEach(function() {
    clock.restore();
  });


  describe('.create', function() {

    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(QueryToFile);
    });

  });


  describe('#run', function() {
    let result;

    beforeEach(function*() {
      this.sandbox.stub(BigQuery, 'create').returns(new BigQuery);
      this.sandbox.stub(BigQuery.prototype, 'table').returns(table);
      this.sandbox.stub(QueryToTable, 'create').returns(new QueryToTable);
      this.sandbox.stub(QueryToTable.prototype, 'run').resolves();
      this.sandbox.stub(File, 'create').returns('[temp storage file]');
      this.sandbox.stub(TableToFile, 'create').returns(new TableToFile);
      this.sandbox.stub(TableToFile.prototype, 'run').resolves();

      result = yield instance.run('SELECT * FROM `table`', { some: 'options' });
    });


    it('should instantiate a BigQuery client', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });


    it("instantiates a BigQuery Table with basename wrapped 'tmp_' and '_[timestamp]_[customer_id]'", function() {
      expect(BigQuery.prototype.table).to.calledWithExactly('tmp_base_name_1481155200000');
    });


    it("instantiates a QueryToTable with basename wrapped 'tmp_' and '_[timestamp]_[customer_id]'", function() {
      expect(QueryToTable.create).to.calledWithExactly('tmp_base_name_1481155200000');
    });


    it("instantiates a File with basename wrapped 'tmp/' and '_[timestamp]_[customer_id].json.gz'", function() {
      expect(File.create).to.calledWithExactly('tmp/tmp_base_name_1481155200000.json.gz');
    });


    it('instantiates a TableToFile with temp table name and temp file', function() {
      expect(TableToFile.create).to.calledWithExactly('tmp_base_name_1481155200000', '[temp storage file]');
    });


    it('should run the query and save result to table (query to table) with options', function() {
      expect(QueryToTable.prototype.run).to.calledWithExactly('SELECT * FROM `table`', { some: 'options' });
    });


    it('should propagate errors from QueryToTable', function*() {
      QueryToTable.prototype.run.rejects(new Error('QTT Boom!'));

      try {
        yield instance.run();
      } catch (error) {
        expect(error.message).to.eql('QTT Boom!');
        return;
      }

      throw new Error('error should be propagated');
    });


    it('should export the table to storage (table to file) with options', function() {
      expect(TableToFile.prototype.run).to.calledWithExactly();
    });


    it('should propagate errors from TableToFile', function*() {
      TableToFile.prototype.run.rejects(new Error('TTF Boom!'));

      try {
        yield instance.run();
      } catch (error) {
        expect(error.message).to.eql('TTF Boom!');
        return;
      }

      throw new Error('error should be propagated');
    });


    it('should delete the table', function() {
      expect(table.delete).to.calledWithExactly();
    });


    it('should propagate errors from table deletion', function*() {
      table.delete.rejects(new Error('Delete Boom!'));

      try {
        yield instance.run();
      } catch (error) {
        expect(error.message).to.eql('Delete Boom!');
        return;
      }

      throw new Error('error should be propagated');
    });


    it('should return with the file upon success', function() {
      expect(result).to.eql('[temp storage file]');
    });

  });

});
