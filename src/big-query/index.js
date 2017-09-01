'use strict';

process.env.SUPPRESS_NO_CONFIG_WARNING = 'y';

const GoogleCloud = require('google-cloud');
const config = require('config');

class BigQuery {

  static create(datasetName) {
    const client = GoogleCloud.bigquery(config.get('GoogleCloud'));

    return new BigQuery(datasetName || config.get('GoogleCloud.dataset'), client);
  };


  constructor(datasetName, client) {
    this._client = client;
    this._datasetName = datasetName;
  }


  get dataset() {
    return this._client.dataset(this._datasetName);
  }


  table(tableName) {
    return this.dataset.table(tableName);
  }


  createQueryStream(query) {
    return this._client.createQueryStream(query);
  }


  *query(query) {
    return yield this._client.query(query);
  }


  *startQuery(query) {
    return yield this._client.startQuery(query);
  }


  *createTableIfNotExists(table, schema) {
    let [exists] = yield table.exists();

    if (!exists) {
      yield table.create({ schema });
    }
  }


  *dropTableIfExists(table) {
    let [exists] = yield table.exists();

    if (exists) {
      yield table.delete();
    }
  }


  *createTable(tableName, options) {
    let [table] = yield this._client.dataset(this._datasetName).createTable(tableName, options);
    return table;
  }

}


module.exports = BigQuery;
