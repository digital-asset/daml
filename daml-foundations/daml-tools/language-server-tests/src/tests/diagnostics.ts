// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as fs from 'fs';
import * as assert from 'assert';
import * as rxjs from 'rxjs';

/**
 * Helper function to gather diagnostics for the tests.
 */
export function gatherDiagnostics(diagnostics : rxjs.Observable<DT.protocol.PublishDiagnosticsParams>, rootPath: string, expected: any, done) {
    var actual: any = {}; // Gathered diagnostics, keyed by URI
    var markedDone = false;

    let subscription = diagnostics.subscribe({
        next: (diagnostic) => {
            if (diagnostic.uri.search(rootPath) < 0) {
                // Disregard diagnostics from other test-cases
                return;
            }
            diagnostic = DT.truncatePaths(diagnostic);
            if (!actual.hasOwnProperty(diagnostic.uri)) {
                actual[diagnostic.uri] = [];
            }
            actual[diagnostic.uri] = actual[diagnostic.uri].concat(diagnostic.diagnostics);

            const areEqual = DT.utils.deepEqual(expected, actual);
            if (areEqual.equal && !markedDone) {
                markedDone = true;
                done(undefined, [subscription]);
            }
        }
    });

    setTimeout(() => {
        if (markedDone) return;
        try {
            assert.deepEqual(actual, expected);
            done(undefined, [subscription]);
        } catch (e) {
            DT.tryWriteActualJSON(rootPath, actual);
            done(e, [subscription]);
        }
    }, 7000);
}

/// File-based tests
describe("diagnostics", function() {
  this.timeout(30000);
  DT.utils
    .getDirectories(DT.getTestDir("diagnostics"))
    .forEach(
      testPath => DT.testDaml(testPath, (connection) => (done) => {
        testPath = fs.realpathSync(testPath);
        let mainDamlFile = testPath + '/Main.daml';
        let mainDaml: string = DT.utils.readFileAsString(mainDamlFile);
        gatherDiagnostics(connection.diagnostics, testPath, DT.loadExpected(testPath), done);
        connection.openDocument('file://' + mainDamlFile, mainDaml);
    }));
});
