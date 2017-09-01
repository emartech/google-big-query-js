'use strict';

const { createUnzip } = require('zlib');
const split = require('split');
const pumpify = require('pumpify');
const QueryToFile = require('../query-to-file');


class QueryToFileToStream {

  static create(baseName) {
    return new QueryToFileToStream(QueryToFile.create(baseName));
  }


  constructor(queryToFile) {
    this._queryToFile = queryToFile;
  }


  *createQueryStream(query, options) {
    const file = yield this._queryToFile.run(query, options);

    return pumpify.obj(file.createReadStream(), createUnzip(), split(JSON.parse, null, { trailing: false }));
  }

}


module.exports = QueryToFileToStream;
