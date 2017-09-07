'use strict';

const BigQuery = require('../big-query');
const JobRunner = require('../job-runner');

const defaultOptions = {
  sourceFormat: 'NEWLINE_DELIMITED_JSON'
};

class FileToTable {

  static create(file, tableName, schema, options = defaultOptions) {
    const table = BigQuery.create().table(tableName);

    return new FileToTable(file, table, schema, options);
  }


  static createWith(file, table, schema, options = defaultOptions) {
    return new FileToTable(file, table, schema, options);
  }


  constructor(file, table, schema, options) {
    this._file = file;
    this._table = table;
    this._schema = schema;
    this._options = options;
  }


  *run() {
    return yield JobRunner.run(this._table.import(this._file, this._metadata));
  }


  get _metadata() {
    return {
      schema: this._schema,
      sourceFormat: this._options.sourceFormat,
      writeDisposition: 'WRITE_TRUNCATE'
    };
  }

}


module.exports = FileToTable;
