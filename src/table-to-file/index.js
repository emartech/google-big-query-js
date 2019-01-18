'use strict';

const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');


class TableToFile {

  static create(tableName, file, formatOptions = { format: 'JSON', gzip: true }) {
    const table = BigQuery.create().table(tableName);

    return new TableToFile(table, file, formatOptions);
  }


  constructor(table, file, formatOptions = { format: 'JSON', gzip: true }) {
    this._table = table;
    this._file = file;
    this._formatOptions = formatOptions;
  }


  *run() {
    return yield JobRunner.run(this._table.export(this._file, this._formatOptions));
  }

}


module.exports = TableToFile;
