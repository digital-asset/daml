// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as fs from 'fs';
import * as assert from 'assert';
import * as path from 'path';
import * as Rx from 'rxjs'

describe("issue reports", function() {


    // DEL-6241: DAML-GHC not unloading modules from
    // scenario service, leading to internal error
    // when hashes are missing for definitions.
    //
    // The test opens two modules A and B, where
    // A depends on definition in B. A has a failing scenario.
    // The module A is then closed and we expect the
    // scenario diagnostics to clear. Finally we remove
    // the definition from B that A depends on and add
    // a failing scenario to A.
    //
    // Since currently scenario diagnostics are not properly reported
    // this test only validates that the "Ide Error" diagnostic is gone
    // that was shown when the scenario service context update fails.
    DT.testDaml("DEL-6241", connection => done => {
        const uriA = DT.utils.toRequestUri("A.daml");
        const aContents0 = `
daml 1.2 module A where

x : Int
x = 123
`;
        const aContents1 = `
daml 1.2 module A where

-- x is gone
`;

        const uriB = DT.utils.toRequestUri("B.daml");
        const bContents = `
daml 1.2 module B where
import A

testScenario = scenario do
  pure x
`;

        enum steps {
          step1_DiagnosticsForB,
          step2_ClosedB
        };
        var currentStep = steps.step1_DiagnosticsForB;
        var step2timeout = null;

        var step2_seenA = false;
        const step = (diagnostic) => {
          switch(currentStep) {
            case steps.step1_DiagnosticsForB: {
              // step 1: We wait for diagnostics for B before we close it.
              if (diagnostic.uri.indexOf("B.daml") >= 0) {
                step2timeout = setTimeout(done, 10000);
                currentStep = steps.step2_ClosedB;
                connection.closeDocument(uriB);
                connection.changeDocument(
                  uriA, 1, aContents1);
              }
              break;
            }

            case steps.step2_ClosedB: {
              // step 2: We check that no errors arrive for A
              // before 'step2timeout' fires.
              if (diagnostic.uri.indexOf("A.daml") >= 0) {
                const eq = DT.utils.deepEqual(
                  diagnostic.diagnostics.map(d => d.message),
                  []);
                if(!eq.equal) {
                  clearTimeout(step2timeout);
                  done(eq.message);
                }
              }
              break;
            }
          }
        };
        connection
          .diagnostics
          .subscribe(
            { next: step,
              error: (err) => done(err)
            });

        connection.openDocument(uriA, aContents0);
        connection.openDocument(uriB, bContents);
    })

    // NOTE(JM): Disabled due to DEL-5741. Re-enable once fixed.
    /*

    // see https://digitalasset.atlassian.net/browse/PROD-3898
    // Test works by loading a file with an import that hasn't been fully
    // typed (as in written) yet.
    // It then simulates typing the import by changing the file one
    // character at a time.
    // At the end we don't want any diagnostics left for the file.
    DT.testDaml("PROD-3898", (connection) => (done) => {
        const theImport = "tests/issue-reports/PROD-3898/TheImport.daml";
        const main0 = "tests/issue-reports/PROD-3898/Main.daml";
        const main1 = "tests/issue-reports/PROD-3898/Main1.daml";
        const main2 = "tests/issue-reports/PROD-3898/Main2.daml";
        const text0 = fs.readFileSync(main0, 'UTF-8');
        const text1 = fs.readFileSync(main1, 'UTF-8');
        const text2 = fs.readFileSync(main2, 'UTF-8');
        const file0 = DT.utils.toRequestUri(main0);
        const theImportText = fs.readFileSync(theImport, 'UTF-8');
        const theImportFile = DT.utils.toRequestUri(theImport);

        const mainDiagnostics = connection.diagnostics
            .filter((diagnostic) =>  diagnostic.uri.indexOf("Main.daml") >= 0)
            .map((diagnostic) => diagnostic.diagnostics.map((d) => d.message));

        enum steps { step1, step2, step3 }
        let currentStep = steps.step1;

        let expectedDiagnostic1 =
          [ 'Module could not be found: TheImpo'
          ];
        let expectedDiagnostic2 =
          [ 'Module could not be found: TheImpor'
          ];

        mainDiagnostics.subscribe(
            {
                next: (diagnostic) => {
                    switch(currentStep) {
                        case steps.step1: {
                            const eq = DT.utils.deepEqual(diagnostic, expectedDiagnostic1);
                            if(eq.equal) {
                                connection.changeDocument(file0, 1, text1);
                                currentStep = steps.step2;
                            }
                            else {
                                done(eq.message);
                            }
                            break
                        }
                        case steps.step2: {
                            const eq = DT.utils.deepEqual(diagnostic, expectedDiagnostic2);
                            if(eq.equal) {
                                connection.changeDocument(file0, 2, text2);
                                currentStep = steps.step3;
                            }
                            else {
                                done(eq.message);
                            }
                            break
                        }
                        case steps.step3: {
                            const eq = DT.utils.deepEqual(diagnostic, []);
                            if(eq.equal) {
                                done();
                            }
                            else {
                                done(eq.message);
                            }
                            break;
                        }
                    }
                },
                error: (err) => done(err)
            }
        );

        connection.openDocument(theImportFile, theImportText);
        connection.openDocument(file0, text0);

    });

    */
});
