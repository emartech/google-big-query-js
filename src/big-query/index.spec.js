'use strict';

const BigQuery = require('./');
const GoogleCloud = require('google-cloud');
const GoogleBigQuery = GoogleCloud.bigquery;
const GoogleBigQueryDataset = GoogleBigQuery.Dataset;

describe('BigQuery', function() {

  beforeEach(function() {
    this.sandbox.stub(GoogleCloud, 'bigquery').returns(new GoogleBigQuery({}));
  });


  describe('.create', function() {

    it('should return with an instance', function() {
      expect(BigQuery.create()).to.be.an.instanceOf(BigQuery);
    });


    it('should setup a big query client', function() {
      BigQuery.create();

      expect(GoogleCloud.bigquery).to.calledWithExactly({ projectId: 'main_project', dataset: 'main_dataset' });
    });


    it('should setup a big query client (passing the gcloud config untouched, even a dataset was provided', function() {
      BigQuery.create('some_other_dataset');

      expect(GoogleCloud.bigquery).to.calledWithExactly({ projectId: 'main_project', dataset: 'main_dataset' });
    });

  });


  describe('instance', function() {
    let result;

    beforeEach(function() {
      this.sandbox.stub(GoogleBigQuery.prototype, 'query').resolves(['[array of object]']);
      this.sandbox.stub(GoogleBigQuery.prototype, 'startQuery').resolves(['[Job]']);
      this.sandbox.stub(GoogleBigQuery.prototype, 'createQueryStream').returns('[readable stream]');
      this.sandbox.spy(GoogleBigQuery.prototype, 'dataset');
      this.sandbox.stub(GoogleBigQueryDataset.prototype, 'table').returns('[Table]');

    });


    describe('#createQueryStream', function() {

      beforeEach(function() {
        result = BigQuery.create().createQueryStream('[sql or object]');
      });


      it("should call BigQuery's createQueryStream", function() {
        expect(GoogleBigQuery.prototype.createQueryStream).to.calledWithExactly('[sql or object]');
      });


      it('should return with the readable stream', function() {
        expect(result).to.eql('[readable stream]');
      });


      it('should propagate error', function*() {
        GoogleBigQuery.prototype.createQueryStream.throws(new Error('Boom!'));

        try {
          yield BigQuery.create('miss_march').createQueryStream('SELECT 1');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });

    });


    describe('#query', function() {

      beforeEach(function*() {
        result = yield BigQuery.create().query('SELECT 1');
      });


      it('should pass the query to the original query function on the client', function() {
        expect(GoogleBigQuery.prototype.query).to.calledWithExactly('SELECT 1');
      });


      it("should return with the query's result", function() {
        expect(result).to.eql(['[array of object]']);
      });


      it('should propagate error', function*() {
        GoogleBigQuery.prototype.query.rejects(new Error('Boom!'));

        try {
          yield BigQuery.create().query('SELECT 1');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });
    });


    describe('#startQuery', function() {

      beforeEach(function*() {
        result = yield BigQuery.create().startQuery('SELECT 1');
      });


      it('should pass the query to the original statQuery of the client', function() {
        expect(GoogleBigQuery.prototype.startQuery).to.calledWithExactly('SELECT 1');
      });


      it("should return with the query's result", function() {
        expect(result).to.eql(['[Job]']);
      });


      it('should propagate error', function*() {
        GoogleBigQuery.prototype.startQuery.rejects(new Error('Boom!'));

        try {
          yield BigQuery.create().startQuery('SELECT 1');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });

    });

    describe('#dataset (getter)', function() {

      it('should instantiate dataset with the name passed in for .create', function() {
        BigQuery.create('miss_march').dataset;

        expect(GoogleBigQuery.prototype.dataset).to.calledWithExactly('miss_march');
      });


      it('should use the dataset from the config if none was passed in', function() {
        BigQuery.create().dataset;

        expect(GoogleBigQuery.prototype.dataset).to.calledWithExactly('main_dataset');
      });


      it('should return with the dataset instance', function() {
        expect(BigQuery.create().dataset).to.be.an.instanceOf(GoogleBigQueryDataset);
      });

    });


    describe('#table', function() {
      beforeEach(function() {
        result = BigQuery.create('miss_march').table('horsedick_dot_mpeg');
      });


      it('should use the dataset passed in at initialization', function() {
        expect(GoogleBigQuery.prototype.dataset).to.calledWithExactly('miss_march');
      });


      it('should get table from the dataset', function() {
        expect(GoogleBigQueryDataset.prototype.table).to.calledWithExactly('horsedick_dot_mpeg');
      });


      it('should propagate error from dataset', function() {
        GoogleBigQuery.prototype.dataset.restore();
        this.sandbox.stub(GoogleBigQuery.prototype, 'dataset').throws(new Error('Boom!'));

        try {
          result = BigQuery.create('miss_march').table('horsedick_dot_mpeg');
        } catch (error) {
          expect(error.message).to.eql('Boom!');
          return;
        }

        throw new Error('should raise error');
      });


      it('should propagate error from dataset', function() {
        GoogleBigQueryDataset.prototype.table.throws(new Error('Boom!'));

        try {
          result = BigQuery.create('miss_march').table('horsedick_dot_mpeg');
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

      beforeEach(function() {
        GoogleBigQueryDataset.prototype.table.restore();
        table = BigQuery.create('miss_march').table('horsedick_dot_mpeg');

        this.sandbox.stub(table, 'exists').resolves([true]);
        this.sandbox.stub(table, 'create').resolves();
      });


      context('when the table exists', function() {

        beforeEach(function*() {
          yield BigQuery.create('miss_march').createTableIfNotExists(table, schema);
        });


        it("should not create the table if it's already exist", function() {
          expect(table.create).to.not.called;
        });

      });


      context('when the table does not exist', function() {

        beforeEach(function*() {
          table.exists.resolves([false]);

          yield BigQuery.create('miss_march').createTableIfNotExists(table, schema);
        });


        it('should create the table', function*() {
          expect(table.create).to.calledWithExactly({ schema: '[schema of table to create]' });
        });

      });


      context('when an error occurs', function() {

        it('should propagate error from table.create', function*() {
          table.exists.resolves([false]);
          table.create.rejects(new Error("can't create table"));

          try {
            yield BigQuery.create('miss_march').createTableIfNotExists(table, schema);
          } catch (error) {
            expect(error.message).to.eql("can't create table");
            return;
          }

          throw new Error("I'm sad 'cause no error was propagated");
        });

      });

    });


    describe('#dropTableIfExists', function() {
      let table;

      beforeEach(function() {
        GoogleBigQueryDataset.prototype.table.restore();
        table = BigQuery.create('miss_march').table('horsedick_dot_mpeg');

        this.sandbox.stub(table, 'exists');
        this.sandbox.stub(table, 'delete').resolves();
      });


      context('when the table exists', function() {

        beforeEach(function*() {
          table.exists.resolves([true]);

          yield BigQuery.create('miss_march').dropTableIfExists(table);
        });


        it('should drop the table if it exists', function() {
          expect(table.delete).to.called;
        });

      });


      context('when the table does not exist', function() {

        beforeEach(function*() {
          table.exists.resolves([false]);

          yield BigQuery.create('miss_march').dropTableIfExists(table);
        });


        it('should not try to drop the table', function*() {
          expect(table.delete).not.to.be.called;
        });

      });


      context('when an error occurs', function() {

        it('should propagate error from table.create', function*() {
          table.exists.resolves([true]);
          table.delete.rejects(new Error("can't drop table"));

          try {
            yield BigQuery.create('miss_march').dropTableIfExists(table);
          } catch (error) {
            expect(error.message).to.eql("can't drop table");
            return;
          }

          throw new Error("I'm sad 'cause no error was propagated");
        });

      });

    });


    describe('#createTable', function() {
      let dataset;
      let result;
      let options = {
        id: 123
      };
      let tableName = 'myTable';

      beforeEach(function*() {
        dataset = {
          createTable: this.sandbox.stub().resolves(['[TABLE]', '[RESPONSE]'])
        };

        GoogleBigQuery.prototype.dataset.restore();
        this.sandbox.stub(GoogleBigQuery.prototype, 'dataset').returns(dataset);

        result = yield BigQuery.create().createTable(tableName, options);
      });


      it('should use the dataset from the config', function() {
        expect(GoogleBigQuery.prototype.dataset).to.calledWithExactly('main_dataset');
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
