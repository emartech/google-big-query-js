'use strict';

const API = require('./');
const BigQuery = require('./big-query');

describe('API', function() {

  it('is a Bigquery wrapper', function() {
    expect(API).to.be.eql(BigQuery);
  });

});
