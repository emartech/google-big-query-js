'use strict';

process.env.SUPPRESS_NO_CONFIG_WARNING = 'y';
const config = require('config');

require('co-mocha');
const sinon = require('sinon');

const chai = require('chai');

chai.use(require('sinon-chai'));
chai.use(require('chai-subset'));
global.expect = chai.expect;
global.sinon = sinon;

before(function() {
  config.util.setModuleDefaults('GoogleCloud', {
    projectId: 'main_project',
    dataset: 'main_dataset',
    maximumBillingTier: 100
  });
});

beforeEach(function() {
  this.sandbox = sinon.createSandbox();
});

afterEach(function() {
  this.sandbox.restore();
});
