// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as fs from 'fs';
import * as assert from 'assert';
import * as path from 'path';

describe("requests", () => {
  DT.utils
    .getDirectories(DT.getTestDir('requests'))
    .forEach(testPath => DT.testDaml(testPath, (connection) => (done) => {
      testPath = fs.realpathSync(testPath);
      let spec: DT.RequestSpec = JSON.parse(DT.utils.readFileAsString(testPath + '/request.json'));
      let expected: any = DT.loadExpected(testPath);

      // HACK: Rewrite all Paths to absolute paths
      if (spec.payload.textDocument && spec.payload.textDocument.uri) {
          spec.payload.textDocument.uri =
              'file://' + path.join(testPath, spec.payload.textDocument.uri);
      }

      let mainDamlFile = testPath + '/Main.daml';
      if (fs.existsSync(mainDamlFile)) {
          let mainDaml: string = DT.utils.readFileAsString(mainDamlFile);
          connection.openDocument('file://' + mainDamlFile, mainDaml);
      }

      connection
          .sendRequest(spec.method, spec.payload)
          .then((result: any) => {
              result = DT.truncatePaths(result);
              try {
                  assert.deepEqual(result, expected);
                  done();
              } catch (err) {
                  DT.tryWriteActualJSON(testPath, result);
                  done(DT.prettyErrorJSON(err, expected, result));
              }
          }, done);
    }));
});