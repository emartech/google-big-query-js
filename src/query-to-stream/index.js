'use strict';

const BigQuery = require('../big-query');

const DEFAULT_QUERY_PARAMETERS = {
  useLegacySql: false
};


class QueryToStream {

  static create() {
    return new QueryToStream(BigQuery.create());
  }


  constructor(client) {
    this._client = client;
  }


  createReadStream(query, parameters = {}) {
    return this._client.createQueryStream(Object.assign({}, DEFAULT_QUERY_PARAMETERS, parameters, { query }));
  }

}

module.exports = QueryToStream;
