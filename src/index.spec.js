'use strict';

const API = require('./');
const BigQuery = require('./big-query');
const FileToTable = require('./file-to-table');
const QueryToFile = require('./query-to-file');
const QueryToStream = require('./query-to-stream');
const QueryToTable = require('./query-to-table');
const QueryToView = require('./query-to-view');
const StreamToTable = require('./stream-to-table');
const TableToFile = require('./table-to-file');

describe('API', function() {

  it('is a Bigquery wrapper', function() {
    expect(API).to.be.eql(BigQuery);
  });


  describe('#FileToTable', function() {

    it('is a FileToTable', function() {
      expect(API.FileToTable).to.be.eql(FileToTable);
    });

  });


  describe('#QueryToFile', function() {

    it('is a QueryToFile', function() {
      expect(API.QueryToFile).to.be.eql(QueryToFile);
    });

  });


  describe('#QueryToStream', function() {

    it('is a QueryToStream', function() {
      expect(API.QueryToStream).to.be.eql(QueryToStream);
    });

  });


  describe('#QueryToTable', function() {

    it('is a QueryToTable', function() {
      expect(API.QueryToTable).to.be.eql(QueryToTable);
    });

  });


  describe('#QueryToView', function() {

    it('is a QueryToView', function() {
      expect(API.QueryToView).to.be.eql(QueryToView);
    });

  });


  describe('#StreamToTable', function() {

    it('is a StreamToTable', function() {
      expect(API.StreamToTable).to.be.eql(StreamToTable);
    });

  });


  describe('#TableToFile', function() {

    it('is a TableToFile', function() {
      expect(API.TableToFile).to.be.eql(TableToFile);
    });

  });

});
