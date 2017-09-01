'use strict';

const API = require('./');
const BigQuery = require('./big-query');
const QueryToStream = require('./query-to-stream');

describe('API', function() {

  it('is a Bigquery wrapper', function() {
    expect(API).to.be.eql(BigQuery);
  });


  describe('#QueryToStream', function() {

    it('is a QueryToStream', function() {
      expect(API.QueryToStream).to.be.eql(QueryToStream);
    });

  });

});
