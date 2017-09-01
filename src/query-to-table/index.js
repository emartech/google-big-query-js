'use strict';

const config = require('config');
const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');


class QueryToTable {

  static create(tableName) {
    return new QueryToTable(BigQuery.create(), tableName);
  }


  constructor(client, tableName) {
    this._client = client;
    this._destinationTable = {
      projectId: config.get('GoogleCloud.projectId'),
      datasetId: config.get('GoogleCloud.dataset'),
      tableId: tableName
    };
  }


  *run(query, params = {}) {
    return yield JobRunner.run(this._client.startQuery(this._getOptions(query, params)));
  }


  _getOptions(query, params) {
    return Object.assign({
      query: query,
      useLegacySql: false,
      destinationTable: this._destinationTable,
      writeDisposition: 'WRITE_TRUNCATE',
      maximumBillingTier: config.get('GoogleCloud.maximumBillingTier')
    }, params);
  }

}


module.exports = QueryToTable;
