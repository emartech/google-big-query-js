'use strict';

const { Transform } = require('stream');
const BigQuery = require('../big-query');


class StreamToTable {

  static create(tableName, tableSchema) {
    const table = BigQuery.create().table(tableName);

    return new StreamToTable(table, tableSchema);
  }


  constructor(table, tableSchema) {
    this._table = table;
    this._tableSchema = tableSchema;
  }


  saveStream(readStream) {
    return new Promise((resolve, reject) => {
      readStream.on('error', reject)
        .pipe(this._transformStream).on('error', reject)
        .pipe(this._writeStream).on('error', reject).on('complete', resolve);
    });
  }


  get _transformStream() {
    return new Transform({
      objectMode: true,
      transform(chunk, enc, next) {
        let error;

        try {
          this.push(JSON.stringify(chunk) + '\n');
        } catch (e) {
          error = e;
        }
        next(error);
      }
    });
  }


  get _writeStream() {
    return this._table.createWriteStream(this._streamMetadata);
  }


  get _streamMetadata() {
    return {
      schema: this._tableSchema,
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      writeDisposition: 'WRITE_TRUNCATE'
    };
  }
}


module.exports = StreamToTable;
