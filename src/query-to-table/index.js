'use strict';

const config = require('config');
const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');


class QueryToTable {

  static create(tableName, dataset) {
    return new QueryToTable(BigQuery.create(dataset), tableName, dataset);
  }


  constructor(client, tableName, dataset = config.get('GoogleCloud.dataset')) {
    this._client = client;
    this._destinationTable = {
      projectId: config.get('GoogleCloud.projectId'),
      datasetId: dataset,
      tableId: tableName
    };
  }


  *run(query, params = {}) {
    return yield JobRunner.run(this._client.createQueryJob(this._getOptions(query, params)));
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
