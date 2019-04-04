// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as assert from 'assert';

/// Test for the scenario results delivered as a virtual resource.
DT.testDaml('scenario-virtual-resource', (connection) => (done) => {
    // Register for diagnostics to catch unexpected errors.
    let subscription = connection.diagnostics.subscribe(
        {
            next: (diagnostic) =>
            {
                try {
                    assert.equal(diagnostic.diagnostics.length, 0);
                } catch(err) {
                    done(err, [subscription]);
                }
            }
        }
    );

    // Open a DAML file with a scenario.
    connection.openDocument(
      'file:///Main.daml',
      'daml 1.2 module Main where\nmain = scenario $ assert (True == True)\n');



    // Ask for code lenses and open the first lens, that is the scenario results.
    connection
        .codeLens('file:///Main.daml')
        .then(lenses => {

            try {
                assert.equal(lenses.length, 1);
                assert.equal(lenses[0].command.command, 'daml.showResource');
                assert.equal(lenses[0].command.arguments.length, 2);
                const uri = lenses[0].command.arguments[1];

                // Register for the didChange notification to receive the results.
                connection.onNotification(
                    DT.DidChangeVirtualResourceNotification,
                    note => {
                        assert.ok(note.hasOwnProperty('contents'));
                        assert.ok(note['contents'].length > 10);
                        assert.ok(note.hasOwnProperty('uri'))
                        assert.ok(note['uri'] === uri);
                        connection.closeDocument(lenses[0].command.arguments[1]);
                        done(undefined, [subscription])
                    }
                );


                // Open the virtual resource to trigger the above notification
                connection.openDocument(lenses[0].command.arguments[1], '');
            } catch(err) {
                done(err, [subscription]);
            }
        });
});
