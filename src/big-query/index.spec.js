'use strict';

const config = require('config');
const BigQuery = require('./');

describe('BigQuery', function() {

  describe('.create', function() {

    it('should return with an instance', function() {
      expect(BigQuery.create()).to.be.an.instanceOf(BigQuery);
    });

  });


  describe('instance', function() {
    let result;
    let client;
    let dataset;
    let tableStub;

    let createBigQueryInstance = function(datasetName = config.get('GoogleCloud.dataset')) {
      tableStub = {
        exists: this.sandbox.stub(),
        create: this.sandbox.stub().resolves(),
        delete: this.sandbox.stub().resolves()

      };
      dataset = {
        table: this.sandbox.stub().returns(tableStub),
        createTable: this.sandbox.stub()
      };
      client = {
        query: this.sandbox.stub().resolves(['[array of object]']),
        createQueryJob: this.sandbox.stub().resolves(['[Job]']),
        createQueryStream: this.sandbox.stub().returns('[readable stream]'),
        dataset: this.sandbox.stub().returns(dataset)
      };
      return new BigQuery(datasetName, client);
    };

    beforeEach(function() {
      createBigQueryInstance = createBigQueryInstance.bind(this);
    });


    describe('#createQueryStream', function() {

      beforeEach(function() {
        result = createBigQueryInstance().createQueryStream('[sql or object]');
      });


      it("should call BigQuery's createQueryStream", function() {
        expect(client.createQueryStream).to.calledWithExactly('[sql or object]');
      });


      it('should return with the readable stream', function() {
        expect(result).to.eql('[readable stream]');
      });


      it('should propagate error', function*() {
        const instance = createBigQueryInstance('another_dataset_name');
        client.createQueryStream.throws(new Error('Boom!'));

        try {
          instance.createQueryStream('SELECT 1');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });

    });


    describe('#query', function() {

      beforeEach(function*() {
        result = yield createBigQueryInstance().query('SELECT 1');
      });


      it('should pass the query to the original query function on the client', function() {
        expect(client.query).to.calledWithExactly('SELECT 1');
      });


      it("should return with the query's result", function() {
        expect(result).to.eql(['[array of object]']);
      });


      it('should propagate error', function*() {
        const instance = createBigQueryInstance();
        client.query.rejects(new Error('Boom!'));

        try {
          yield instance.query('SELECT 1');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });
    });


    describe('#createQueryJob', function() {

      beforeEach(function*() {
        result = yield createBigQueryInstance().createQueryJob('SELECT 1');
      });


      it('should pass the query to the original statQuery of the client', function() {
        expect(client.createQueryJob).to.calledWithExactly('SELECT 1');
      });


      it("should return with the query's result", function() {
        expect(result).to.eql(['[Job]']);
      });


      it('should propagate error', function*() {
        const instance = createBigQueryInstance();
        client.createQueryJob.rejects(new Error('Boom!'));

        try {
          yield instance.createQueryJob('SELECT 1');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });

    });

    describe('#dataset (getter)', function() {

      it('should instantiate dataset with the name passed in for .create', function() {
        createBigQueryInstance('another_dataset_name').dataset;

        expect(client.dataset).to.calledWithExactly('another_dataset_name');
      });


      it('should use the dataset from the config if none was passed in', function() {
        createBigQueryInstance().dataset;

        expect(client.dataset).to.calledWithExactly('main_dataset');
      });


      it('should return with the dataset instance', function() {
        expect(createBigQueryInstance().dataset).to.be.eql(dataset);
      });

    });


    describe('#table', function() {
      beforeEach(function() {
        result = createBigQueryInstance('another_dataset_name').table('a_table_name');
      });


      it('should use the dataset passed in at initialization', function() {
        expect(client.dataset).to.calledWithExactly('another_dataset_name');
      });


      it('should get table from the dataset', function() {
        expect(dataset.table).to.calledWithExactly('a_table_name');
      });


      it('should propagate error from dataset', function() {
        const instance = createBigQueryInstance('another_dataset_name');
        client.dataset.throws(new Error('Boom!'));

        try {
          result = instance.table('a_table_name');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });


      it('should propagate error from dataset', function() {
        const instance = createBigQueryInstance('another_dataset_name');
        dataset.table.throws(new Error('Boom!'));

        try {
          result = instance.table('a_table_name');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });

    });


    describe('#createTableIfNotExists', function() {
      let table;
      let schema = '[schema of table to create]';
      let instance;

      beforeEach(function() {
        instance = createBigQueryInstance('another_dataset_name');
        table = instance.table('a_table_name');
      });


      context('when the table exists', function() {

        beforeEach(function*() {
          tableStub.exists.resolves([true]);
          yield instance.createTableIfNotExists(table, schema);
        });


        it("should not create the table if it's already exist", function() {
          expect(tableStub.create).to.not.called;
        });

      });


      context('when the table does not exist', function() {

        beforeEach(function*() {
          tableStub.exists.resolves([false]);
          yield instance.createTableIfNotExists(table, schema);
        });


        it('should create the table', function*() {
          expect(tableStub.create).to.calledWithExactly({ schema: '[schema of table to create]' });
        });

      });


      context('when an error occurs', function() {

        it('should propagate error from table.create', function*() {
          tableStub.exists.resolves([false]);
          tableStub.create.rejects(new Error("can't create table"));

          try {
            yield instance.createTableIfNotExists(table, schema);
          } catch (error) {
            expect(error.message).to.eql("can't create table");
            return;
          }

          throw new Error("I'm sad 'cause no error was propagated");
        });

      });

    });


    describe('#dropTableIfExists', function() {
      let instance;
      let table;

      beforeEach(function() {
        instance = createBigQueryInstance('another_dataset_name');
        table = instance.table('a_table_name');
      });


      context('when the table exists', function() {

        beforeEach(function*() {
          tableStub.exists.resolves([true]);

          yield instance.dropTableIfExists(table);
        });


        it('should drop the table if it exists', function() {
          expect(tableStub.delete).to.called;
        });

      });


      context('when the table does not exist', function() {

        beforeEach(function*() {
          tableStub.exists.resolves([false]);

          yield instance.dropTableIfExists(table);
        });


        it('should not try to drop the table', function*() {
          expect(tableStub.delete).not.to.be.called;
        });

      });


      context('when an error occurs', function() {

        it('should propagate error from table.create', function*() {
          tableStub.exists.resolves([true]);
          tableStub.delete.rejects(new Error("can't drop table"));

          try {
            yield instance.dropTableIfExists(table);
          } catch (error) {
            expect(error.message).to.eql("can't drop table");
            return;
          }

          throw new Error("I'm sad 'cause no error was propagated");
        });

      });

    });


    describe('#createTable', function() {
      let result;

      beforeEach(function*() {
        const instance = createBigQueryInstance();
        dataset.createTable.resolves(['[TABLE]', '[RESPONSE]']);
        result = yield instance.createTable('myTable', { id: 123 });
      });


      it('should use the dataset from the config', function() {
        expect(client.dataset).to.calledWithExactly('main_dataset');
      });


      it('should create a table on the dataset with the given options', function*() {
        expect(dataset.createTable).to.have.been.calledWithExactly('myTable', { id: 123 });
      });

      it('should returned the new table object', function*() {
        expect(result).to.eql('[TABLE]');
      });

    });

  });

});
