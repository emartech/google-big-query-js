'use strict';

const API = require('./');
const BigQuery = require('./big-query');
const QueryToStream = require('./query-to-stream');
const QueryToView = require('./query-to-view');

describe('API', function() {

  it('is a Bigquery wrapper', function() {
    expect(API).to.be.eql(BigQuery);
  });


  describe('#QueryToStream', function() {

    it('is a QueryToStream', function() {
      expect(API.QueryToStream).to.be.eql(QueryToStream);
    });

  });


  describe('#QueryToView', function() {

    it('is a QueryToView', function() {
      expect(API.QueryToView).to.be.eql(QueryToView);
    });

  });

});
