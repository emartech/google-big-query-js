'use strict';

const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');


class FileToTable {

  static create(file, tableName, schema) {
    const table = BigQuery.create().table(tableName);

    return new FileToTable(file, table, schema);
  }


  static createWith(file, table, schema) {
    return new FileToTable(file, table, schema);
  }


  constructor(file, table, schema) {
    this._file = file;
    this._table = table;
    this._schema = schema;
  }


  *run() {
    return yield JobRunner.run(this._table.import(this._file, this._metadata));
  }


  get _metadata() {
    return {
      schema: this._schema,
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      writeDisposition: 'WRITE_TRUNCATE'
    };
  }

}


module.exports = FileToTable;
