'use strict';

const { Writable } = require('stream');
const from2 = require('from2');
const from2array = require('from2-array');
const BigQuery = require('../big-query');
const StreamToTable = require('./');

describe('StreamToTable', function() {
  const tableName = 'test_table_123';
  const tableSchema = {
    fields: [
      { name: 'int_field', type: 'INTEGER', mode: 'REQUIRED' },
      { name: 'float_field', type: 'FLOAT', mode: 'REQUIRED' }
    ]
  };


  beforeEach(function() {
    this.sandbox.stub(BigQuery, 'create').returns(new BigQuery());
  });


  describe('.create', function() {
    let subject;

    beforeEach(function() {
      this.sandbox.stub(BigQuery.prototype, 'table');

      subject = StreamToTable.create(tableName, tableSchema);
    });


    it('should return a class instance', function() {
      expect(subject).to.be.an.instanceOf(StreamToTable);
    });


    it('should properly instantiate a BigQuery client', function() {
      expect(BigQuery.create).to.calledWithExactly();
    });


    it('should properly create a BigQuery Table instance', function() {
      expect(BigQuery.prototype.table).to.calledWithExactly('test_table_123');
    });

  });


  describe('#saveStream', function() {
    let table;
    let results;
    let writeStream;

    const createWriteStream = (error) => {
      const stream = new Writable({
        write(chunk, enc, cb) {
          results.push(chunk.toString());
          cb(error);
        }
      });

      return stream.on('finish', () => writeStream.emit('complete', '[Job object]'));
    };


    beforeEach(function() {
      results = [];
      writeStream = createWriteStream();

      table = { createWriteStream: this.sandbox.stub().returns(writeStream) };
      this.sandbox.stub(BigQuery.prototype, 'table').returns(table);
    });


    it('should create a properly configured write stream for the table', function*() {
      StreamToTable.create(tableName, tableSchema).saveStream(from2array.obj([]));

      expect(table.createWriteStream).to.have.been.calledWithExactly({
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        writeDisposition: 'WRITE_TRUNCATE',
        schema: {
          fields: [
            { name: 'int_field', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'float_field', type: 'FLOAT', mode: 'REQUIRED' }
          ]
        }
      });
    });


    it('should pipe the new-line delimited JSON strings towards BigQuery (the writeStream)', function*() {
      yield StreamToTable.create(tableName, tableSchema).saveStream(from2array.obj([
        { int_field: 1, float_field: 0.79 },
        { int_field: 2, float_field: 0.54 }
      ]));

      expect(results).to.eql([
        '{"int_field":1,"float_field":0.79}\n',
        '{"int_field":2,"float_field":0.54}\n'
      ]);
    });


    it("should return with the new created Job's id", function*() {
      let job = yield StreamToTable.create(tableName, tableSchema).saveStream(from2array.obj(['item']));

      expect(job).to.eql('[Job object]');
    });


    it('should propagate the error from the writeStream', function*() {
      writeStream = createWriteStream(new Error('error in writeStream'));
      table.createWriteStream = this.sandbox.stub().returns(writeStream);

      try {
        yield StreamToTable.create(tableName, tableSchema).saveStream(from2array.obj(['item']));
      } catch (e) {
        expect(e.message).to.eql('error in writeStream');
        return;
      }

      throw new Error('should propagate error');
    });


    it('should propagate the error from the transformStream (JSON.stringify fails (is it possible?))', function*() {
      this.sandbox.stub(JSON, 'stringify').throws(Error('error in transformStream'));

      try {
        yield StreamToTable.create(tableName, tableSchema).saveStream(from2array.obj(['item']));
      } catch (e) {
        expect(e.message).to.eql('error in transformStream');
        return;
      }

      throw new Error('should propagate error');
    });


    it('should propagate the error from the (input) readStream', function*() {
      try {
        yield StreamToTable.create(tableName, tableSchema)
          .saveStream(from2((size, next) => next(new Error('error in readStream'))));
      } catch (e) {
        expect(e.message).to.eql('error in readStream');
        return;
      }

      throw new Error('should propagate error');
    });

  });

});
