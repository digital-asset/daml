// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as assert from 'assert';

// TODO(DEL-5737): We don't have any progress reporting in VS Code for
// DAML 1.2 yet. Once we have it, enable the test again.
/// Test for the  file validations report.
/*
DT.testDaml('file-validation-report', (connection) => (done) => {
    var subs = [];
    // Register for diagnostics to catch unexpected errors.
    let subscription = connection.diagnostics.subscribe({
        next: (diagnostic) => {
            try {
                assert.equal(diagnostic.diagnostics.length, 0);
            } catch(err) {
                done(err, subs);
            }
        }
    });
    subs.push(subscription);
    let subscription2 = connection.allValidationsPassed.subscribe({
        next: () => {
            connection.closeDocument('file:///Main.daml');
            done(undefined, subs);
        }
    });
    subs.push(subscription2);

    // Open a DAML file with a scenario.
    connection.openDocument('file:///Main.daml',
      'daml 1.2 module Main where\nsimple = scenario do\n  assert (True == True)');
});
*/
