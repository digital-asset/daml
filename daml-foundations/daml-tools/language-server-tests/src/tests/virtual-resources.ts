// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as DT from '../daml-test';
import * as assert from 'assert';
import * as fs from 'fs';
import * as url from 'url';
import * as querystring from 'querystring';
import * as path from 'path';

describe("virtual resources", () => {
  DT.utils
    .getDirectories(DT.getTestDir('virtual-resources'))
    .forEach(testPath => DT.testDaml(testPath, (connection) => (done) => {
      testPath = path.resolve(testPath);
      let spec: DT.VirtualResourceSpec = JSON.parse(DT.utils.readFileAsString(testPath + '/request.json'));
      let expected: string = DT.utils.readFileAsString(testPath + '/EXPECTED.html');
      if (spec.document) {
          let parsedUrl = url.parse(spec.document);
          if(parsedUrl.protocol == "daml:") {
              if(parsedUrl.host == "compiler") {
                  let query = querystring.parse(parsedUrl.query);
                  query.file = path.resolve(query.file);
                  let absoluteUrl =
                      parsedUrl.protocol + '//' + parsedUrl.host + '?' + querystring.stringify(query);
                  spec.document = absoluteUrl;
              }
          }
      }


      connection.onNotification(
          DT.DidChangeVirtualResourceNotification,
          (note : DT.VirtualResourceParams) => {
              assert.ok(note.hasOwnProperty('contents'));
              let actual = DT.truncatePaths(note.contents);
              try {
                assert.ok(note.contents.length > 10);
                assert.ok(note.hasOwnProperty('uri'))
                assert.deepEqual(actual,expected);
                done();
              } catch(err) {
                 DT.tryWriteActual(testPath + "/ACTUAL.html", actual);
                 done(DT.prettyError(err, expected, actual));
              }
          }
      );

      let mainDamlFile : string = testPath + '/Main.daml';
      connection.openDocument(DT.utils.toRequestUri(mainDamlFile), DT.utils.readFileAsString(mainDamlFile));
      connection.openDocument(spec.document, '');
  }));
});
