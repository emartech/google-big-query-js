'use strict';

const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');


class TableToFile {

  static create(tableName, file) {
    const table = BigQuery.create().table(tableName);

    return new TableToFile(table, file);
  }


  constructor(table, file) {
    this._table = table;
    this._file = file;
  }


  *run() {
    return yield JobRunner.run(this._table.export(this._file, { format: 'JSON', gzip: true }));
  }

}


module.exports = TableToFile;
