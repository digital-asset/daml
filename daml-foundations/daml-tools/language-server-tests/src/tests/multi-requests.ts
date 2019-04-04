// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as fs from 'fs';
import * as assert from 'assert';
import * as path from 'path';

function multiRequest(connection : DT.DamlConnection, requests : Array<DT.RequestSpec>) : PromiseLike<any[]> {
    function multiRequestImpl(requests : Array<DT.RequestSpec>, acc : Array<any>) : PromiseLike<any[]> {
        if(requests.length === 0) {
            return Promise.resolve(acc);
        }
        else {
            let request = requests[0];
            return connection.sendRequest(request.method, request.payload).then(res => {
                if(request.description) {
                    res = {
                        description: request.description,
                        result: res
                    }
                }
                let remainingRequests = requests.slice(1);
                return multiRequestImpl(remainingRequests, acc.concat([res]));
            });
        }
    }
    return multiRequestImpl(requests, []);
}

describe("multi requests", function() {
  this.timeout(30000);
  DT.utils
    .getDirectories(DT.getTestDir("multi-requests"))
    .forEach(testPath => DT.testDaml(testPath, (connection) => (done) => {
      testPath = fs.realpathSync(testPath);
      let specs: Array<DT.RequestSpec> = JSON.parse(DT.utils.readFileAsString(testPath + '/requests.json'));
      let expected: any = DT.loadExpected(testPath);

      specs.forEach(spec => {
          if(spec.payload.textDocument && spec.payload.textDocument.uri) {
              spec.payload.textDocument.uri =
              'file://' + path.join(testPath, spec.payload.textDocument.uri);
          }
      });

      let files : Array<string> = DT.utils.unique(specs.map(spec => {
          try {
              return path.normalize(spec.payload.textDocument.uri).replace(/file:/,'')
          }
          catch(_) {
              return "";
          }
      }).filter(file => file !== ""));

      files.forEach(file => {
          if (fs.existsSync(file)) {
              let fileContent: string = DT.utils.readFileAsString(file);
              connection.openDocument('file:' + file, fileContent);
          }
      });

      multiRequest(connection, specs).then((results) => {
          results = DT.truncatePaths(results);
          try {
              assert.deepEqual(results, expected);
              done();
          } catch (err) {
              DT.tryWriteActualJSON(testPath, results);
              done(err);
          }
      })
  }));
});
