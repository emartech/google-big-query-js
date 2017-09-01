'use strict';

const QueryToStream = require('./');
const from2array = require('from2-array');
const BigQuery = require('../big-query');

describe('QueryToStream', function() {
  let subject;
  let bqClient;
  let readableObjectStream;

  beforeEach(function() {
    bqClient = { createQueryStream: this.sandbox.stub() };
    this.sandbox.stub(BigQuery, 'create').returns(new BigQuery('datasetId', bqClient));

    subject = QueryToStream.create();
  });


  describe('.create', function() {

    it('should return with an instance of QueryToStream', function() {
      expect(subject).to.be.an.instanceOf(QueryToStream);
    });


    it('should create a bigquery instance', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });

  });


  describe('#createReadStream', function() {
    let result;

    context('when there is no error', function() {

      beforeEach(function*() {
        readableObjectStream = from2array.obj([
          { contact_id: 10, sales_amount: 12 },
          { contact_id: 20, sales_amount: 2 }
        ]);

        bqClient.createQueryStream.returns(readableObjectStream);
      });

      context('and a query is passed as argument', function() {

        beforeEach(function() {
          result = subject.createReadStream('SELECT 1');
        });

        it('should start the query on the BigQuery client', function() {
          expect(bqClient.createQueryStream).to.have.been.calledWithExactly({ query: 'SELECT 1', useLegacySql: false });
        });

        it('should return with the query result stream', function() {
          expect(result).to.eq(readableObjectStream);
        });

      });

      context('when additional query parameters are passed', function() {

        beforeEach(function() {
          result = subject.createReadStream('SELECT 1', { doMeAFavor: true });
        });

        it('should pass optional parameters to the the BigQuery client', function() {
          expect(bqClient.createQueryStream).to.have.been.calledWithExactly({
            query: 'SELECT 1',
            useLegacySql: false,
            doMeAFavor: true
          });
        });

      });


      context('when query parameters that differ from default values are passed', function() {

        beforeEach(function() {
          result = subject.createReadStream('SELECT 1', { useLegacySql: true });
        });

        it('should use the overriding values instead of defaults', function() {
          expect(bqClient.createQueryStream).to.have.been.calledWithExactly({ query: 'SELECT 1', useLegacySql: true });
        });

      });

    });

  });

});
