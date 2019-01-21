'use strict';

const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');


class TableToFile {

  static create(tableName, file, format = 'JSON') {
    const table = BigQuery.create().table(tableName);

    return new TableToFile(table, file, format);
  }


  constructor(table, file, format = 'JSON') {
    this._table = table;
    this._file = file;
    if (format === 'JSON') {
      this._formatOptions = { format: 'JSON', gzip: true };
    } else if (format === 'CSV') {
      this._formatOptions = { format: 'CSV', gzip: false };
    } else {
      throw Error('Unknown file format');
    }
  }


  *run() {
    return yield JobRunner.run(this._table.export(this._file, this._formatOptions));
  }

}


module.exports = TableToFile;
