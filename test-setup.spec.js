'use strict';

process.env.SUPPRESS_NO_CONFIG_WARNING = 'y';
const config = require('config');

require('co-mocha');
const sinon = require('sinon');

const chai = require('chai');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
global.expect = chai.expect;
global.sinon = sinon;

before(function() {
  config.util.setModuleDefaults('GoogleCloud', { projectId: 'main_project', dataset: 'main_dataset' });
});

beforeEach(function() {
  this.sandbox = sinon.sandbox.create();
});

afterEach(function() {
  this.sandbox.restore();
});
