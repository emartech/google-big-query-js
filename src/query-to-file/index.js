'use strict';

const { File } = require('@emartech/google-cloud-storage');
const QueryToTable = require('../query-to-table');
const TableToFile = require('../table-to-file');
const BigQuery = require('../big-query');


class QueryToFile {

  static create(baseName) {
    const tableName = `tmp_${baseName}_${+new Date}`;

    return new QueryToFile(tableName);
  }


  constructor(tableName) {
    this._tableName = tableName;
  }


  *run(query, options) {
    const file = File.create(`tmp/${this._tableName}.json.gz`);

    yield QueryToTable.create(this._tableName).run(query, options);
    yield TableToFile.create(this._tableName, file).run();
    yield BigQuery.create().table(this._tableName).delete();

    return file;
  }

}


module.exports = QueryToFile;
