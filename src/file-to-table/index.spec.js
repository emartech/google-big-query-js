'use strict';

const EventEmitter = require('events');
const FileToTable = require('./');
const BigQuery = require('../big-query');


describe('FileToTable', function() {
  const table = { name: 'table_name', schema: { fields: ['schema'] } };
  const file = '[file]';
  let tableStub;

  beforeEach('mock big query', function() {
    tableStub = { import: this.sandbox.stub() };
    this.sandbox.stub(BigQuery, 'create').returns(new BigQuery());
    this.sandbox.stub(BigQuery.prototype, 'table').returns(tableStub);
  });


  describe('.create', function() {
    let instance;

    beforeEach(function() {
      instance = FileToTable.create(file, table.name, table.schema);
    });


    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(FileToTable);
    });


    it('should instantiate a BigQuery client', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });


    it('should instantiate a BigQuery Table object with proper table name', function() {
      expect(BigQuery.prototype.table).to.calledWithExactly('table_name');
    });

  });


  describe('.createWith', function() {
    let instance;

    beforeEach(function() {
      instance = FileToTable.createWith(file, tableStub, table.schema);
    });


    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(FileToTable);
    });

  });


  describe('#run', function() {
    const job = new EventEmitter;
    let result;
    let apiResponse;
    let jobMetadata;

    const setStubs = () => {
      apiResponse = { status: { state: 'IN_PROGRESS' } };
      jobMetadata = { status: { state: 'DONE' } };
      tableStub.import.resolves([job, apiResponse]);
      setTimeout(() => job.emit('complete', jobMetadata));
    };

    context('when FileToTable created with table name', function() {
      context('when source format should be CSV', function() {
        beforeEach(function() {
          setStubs();
        });

        it('should send \'CSV\' as source format', function*() {
          yield FileToTable.create(file, table.name, table.schema, {
            sourceFormat: 'CSV'
          }).run();

          expect(tableStub.import).to.calledWithExactly('[file]', {
            schema: { fields: ['schema'] },
            sourceFormat: 'CSV',
            writeDisposition: 'WRITE_TRUNCATE'
          });
        });
      });


      context('when source format should be json', function() {

        beforeEach(function* () {
          setStubs();
          result = yield FileToTable.create(file, table.name, table.schema).run();
        });


        it('should pass the proper query object', function() {
          expect(tableStub.import).to.calledWithExactly('[file]', {
            schema: { fields: ['schema'] },
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_TRUNCATE'
          });
        });


        it('should propagate error if starting the import job fails', function*() {
          tableStub.import.rejects(new Error('import creation failed'));

          try {
            yield FileToTable.create().run();
          } catch (e) {
            expect(e.message).to.eql('import creation failed');
            return;
          }

          throw new Error('should propagate error');
        });


        it("should return with the query job's result", function() {
          expect(result).to.eql({ status: { state: 'DONE' } });
        });


        it('should propagate error of the import job', function*() {
          try {
            setTimeout(() => job.emit('error', new Error('import job failed')));
            yield FileToTable.create().run();
          } catch (e) {
            expect(e.message).to.eql('import job failed');
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
            yield FileToTable.create().run();
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


    context('when FileToTable created with table instance', function() {
      context('when source format should be CSV', function() {
        beforeEach(function*() {
          setStubs();

          result = yield FileToTable.createWith(file, tableStub, table.schema, {
            sourceFormat: 'CSV'
          }).run();
        });

        it('works', function() {
          expect(tableStub.import).to.calledWithExactly('[file]', {
            schema: { fields: ['schema'] },
            sourceFormat: 'CSV',
            writeDisposition: 'WRITE_TRUNCATE'
          });
        });
      });


      context('when source format should be json', function() {
        beforeEach(function* () {
          setStubs();

          result = yield FileToTable.createWith(file, tableStub, table.schema).run();
        });


        it('should pass the proper query object', function() {
          expect(tableStub.import).to.calledWithExactly('[file]', {
            schema: { fields: ['schema'] },
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_TRUNCATE'
          });
        });


        it('should propagate error if starting the import job fails', function* () {
          tableStub.import.rejects(new Error('import creation failed'));

          try {
            yield FileToTable.create().run();
          } catch (e) {
            expect(e.message).to.eql('import creation failed');
            return;
          }

          throw new Error('should propagate error');
        });


        it("should return with the query job's result", function() {
          expect(result).to.eql({ status: { state: 'DONE' } });
        });


        it('should propagate error of the import job', function* () {
          try {
            setTimeout(() => job.emit('error', new Error('import job failed')));
            yield FileToTable.create().run();
          } catch (e) {
            expect(e.message).to.eql('import job failed');
            return;
          }

          throw new Error('should propagate error');
        });


        it('should throw error if the query contains errors', function* () {
          apiResponse.status.errorResult = {
            reason: 'invalid',
            location: 'quantile_limits',
            message: 'Table name cannot be resolved: dataset name is missing.'
          };

          try {
            setTimeout(() => job.emit('complete', jobMetadata));
            yield FileToTable.create().run();
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

  });

});
