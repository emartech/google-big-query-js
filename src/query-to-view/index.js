'use strict';

const config = require('config');

const BigQuery = require('../big-query');

class QueryToView {

  static create(viewName) {
    return new QueryToView(BigQuery.create(), viewName);
  }


  constructor(client, viewName) {
    this._client = client;
    this._tableId = viewName;
    this._destinationTable = {
      projectId: config.get('GoogleCloud.projectId'),
      datasetId: config.get('GoogleCloud.dataset'),
      tableId: viewName
    };
  }


  *run(query) {
    return yield this._client.createTable(this._tableId, this._getOptions(query));
  }


  _getOptions(query) {
    return {
      id: this._tableId,
      kind: 'bigquery#table',
      tableReference: this._destinationTable,
      view: {
        query: query,
        useLegacySql: false
      }
    };
  }

}


module.exports = QueryToView;
