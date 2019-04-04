// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

const Mocha = require('mocha');
const mocha_jenkins_reporter = require('mocha-jenkins-reporter');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

function main() {
  const mocha = new Mocha(reporter = mocha_jenkins_reporter);

  fs.readdirSync(global.test_srcdir).filter(function(file) {
    // For some reason the virtual-resources test times out.
    return file.substr(-3) === '.js';
  }).forEach(function(file) {
    mocha.addFile(path.join(global.test_srcdir, file));
  });

  mocha.run(exitMocha);
}

// Copied from mocha since it is not exported.
const exitMocha = code => {
  const clampedCode = Math.min(code, 255);
  let draining = 0;

  // Eagerly set the process's exit code in case stream.write doesn't
  // execute its callback before the process terminates.
  process.exitCode = clampedCode;

  // flush output for Node.js Windows pipe bug
  // https://github.com/joyent/node/issues/6247 is just one bug example
  // https://github.com/visionmedia/mocha/issues/333 has a good discussion
  const done = () => {
    if (!draining--) {
      process.exit(clampedCode);
    }
  };

  const streams = [process.stdout, process.stderr];

  streams.forEach(stream => {
    // submit empty write request and wait for completion
    draining += 1;
    stream.write('', done);
  });

  done();
};

if (require.main === module) {
  // The tests work with relative paths so basedir is set such that we find the tests at tests/*
  const basedir = path.resolve(process.argv[2]);

  // Change directory to resolve relative paths
  if (process.env['TEST_SRCDIR']) {
    process.chdir(process.env['TEST_SRCDIR']);
  }
  global.damlc_path = path.resolve(process.argv[3]);
  global.test_srcdir = path.resolve(process.argv[4]);

  // Now change to basedir
  process.chdir(basedir);

  // Bazel cannot handle files with spaces properly so we rename it here.
  fs.renameSync("tests/virtual-resources/spaces_in_path", "tests/virtual-resources/spaces in path");
  main();
}
