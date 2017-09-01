'use strict';

class JobRunner {

  static *run(yieldable) {
    return yield new JobRunner().run(yieldable);
  }


  *run(yieldable) {
    let job = yield this._waitForIt(yield this._createJob(yieldable));
    this._checkStatus(job);

    return job;
  }


  *_createJob(yieldable) {
    const [job, apiResponse] = yield yieldable;
    this._checkStatus(apiResponse);

    return job;
  }


  _waitForIt(job) {
    return new Promise(function(resolve, reject) {
      job.on('error', reject);
      job.on('complete', resolve);
    });
  }


  _checkStatus(metadata) {
    if (metadata && metadata.status && metadata.status.errorResult) {
      throw new Error(JSON.stringify(metadata.status.errorResult));
    }
  }

}


module.exports = JobRunner;
