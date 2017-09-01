'use strict';

require('co-mocha');
const sinon = require('sinon');

const chai = require('chai');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
global.expect = chai.expect;
global.sinon = sinon;

beforeEach(function() {
  this.sandbox = sinon.sandbox.create();
});

afterEach(function() {
  this.sandbox.restore();
});
