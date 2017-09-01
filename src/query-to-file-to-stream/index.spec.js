'use strict';

const { createGzip } = require('zlib');
const from2string = require('from2-string');
const streamToArray = require('stream-to-array');
const QueryToFileToStream = require('./');
const QueryToFile = require('../query-to-file');


describe('QueryToFileToStream', function() {
  let instance;
  let file;
  let readStream;


  beforeEach(function() {
    readStream = from2string(
      '{"customer_id":"1","contact_id":1}\n' +
      '{"customer_id":"1","contact_id":2}\n' +
      '{"customer_id":"1","contact_id":3}\n'
    ).pipe(createGzip());

    file = { createReadStream: this.sandbox.stub().returns(readStream) };
    this.sandbox.stub(QueryToFile, 'create').returns(new QueryToFile);
    this.sandbox.stub(QueryToFile.prototype, 'run').resolves(file);

    instance = QueryToFileToStream.create('base_name');
  });


  describe('.create', function() {

    it('should return with an instance of itself', function() {
      expect(instance).to.be.an.instanceOf(QueryToFileToStream);
    });


    it('should instantiate a QueryToFile', function() {
      expect(QueryToFile.create).to.calledWithExactly('base_name');
    });

  });


  describe('#createQueryStream', function() {
    let stream;

    beforeEach(function*() {
      stream = yield instance.createQueryStream('[query string]', '[query options]');
    });


    it('should pass query and options to QueryToFile', function() {
      expect(QueryToFile.prototype.run).to.calledWithExactly('[query string]', '[query options]');
    });


    it('should propagate errors from query to file', function*() {
      QueryToFile.prototype.run.rejects(new Error('QTF Boom!'));

      try {
        yield instance.createQueryStream();
      } catch (error) {
        expect(error.message).to.eql('QTF Boom!');
        return;
      }

      throw new Error('error should be propagated');
    });


    it('should create a read stream for the file', function() {
      expect(file.createReadStream).to.calledWithExactly();
    });


    it('should return with the read stream', function*() {
      expect(yield streamToArray(stream)).to.eql([
        { customer_id: '1', contact_id: 1 },
        { customer_id: '1', contact_id: 2 },
        { customer_id: '1', contact_id: 3 }
      ]);
    });


    it('should propagate error from zlib', function*() {
      file.createReadStream.returns(from2string('{"customer_id":"1","contact_id":1}\n'));

      try {
        yield streamToArray(yield instance.createQueryStream());
      } catch (error) {
        expect(error.message).to.eql('incorrect header check');
        return;
      }

      throw new Error('error should be propagated');
    });

  });

});
