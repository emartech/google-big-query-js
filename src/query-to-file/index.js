'use strict';

const { File } = require('@emartech/google-cloud-storage');
const QueryToTable = require('../query-to-table');
const TableToFile = require('../table-to-file');
const BigQuery = require('../big-query');


class QueryToFile {

  static create(baseName, formatOptions = { format: 'JSON', gzip: true }, filename = null) {
    const tableName = `tmp_${baseName}_${+new Date}`;

    return new QueryToFile(tableName, formatOptions, filename);
  }


  constructor(tableName, formatOptions = { format: 'JSON', gzip: true }, filename = null) {
    this._tableName = tableName;
    this._formatOptions = formatOptions;
    this._fileName = filename || `tmp/${this._tableName}.json.gz`;
  }


  *run(query, options) {
    const file = File.create(this._fileName);

    yield QueryToTable.create(this._tableName).run(query, options);
    yield TableToFile.create(this._tableName, file, this._formatOptions).run();
    yield BigQuery.create().table(this._tableName).delete();

    return file;
  }

}


module.exports = QueryToFile;
