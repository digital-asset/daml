// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as fs from 'fs';
import * as deepEqual from 'deep-equal';
import * as assert from 'assert';
import { NotificationType } from 'vscode-jsonrpc';

import * as events from 'events';
import { DamlConnection, WorkspaceValidationsParams } from './connection';
import * as protocol from 'vscode-languageserver-protocol';
import * as utils from './utils';
import * as path from 'path';

export { protocol, utils, events, DamlConnection, WorkspaceValidationsParams };

export interface DamlActionFunction {
    (connection : DamlConnection): (done: (error?: any, subscriptions? : Array<Subscription>) => any) => any | PromiseLike<any>
}

import { Subscription } from 'rxjs'

/**
 * Actual command we will use start the IDE.
 */
var damlCommand : { command : string, args : Array<string> } =
    {
        command: global["damlc_path"],
        args: ['ide']
    };

// Enable tracing. This shows client traces and DAML stderr log output.
const ENABLE_TRACING = false;

// Default test timeout. This is a high value mostly due to the fact that starting
// "damlc ide" + the scenario service can be slow on a busy machine, and we do this for
// each test separately.
const TEST_TIMEOUT = 40000;

export interface RequestSpec {
    description?: string
    method: string;
    payload: any;
}

export interface VirtualResourceSpec {
    document: string;
}

export var DidChangeVirtualResourceNotification =
  new NotificationType<VirtualResourceParams, void>('daml/virtualResource/didChange');

export interface VirtualResourceParams {
    contents : string;
    uri : string;
}

export function loadExpected(path: string): any {
    try {
        return JSON.parse(utils.readFileAsString(path + '/EXPECTED.json'));
    } catch (e) {
        console.log('EXPECTED.json missing or invalid in: ' + path);
        return {};
    }
}

/**
 * Try to write the `ACTUAL` file to disk for the expectation tests.
 * @param path path to the `ACTUAL` file.
 * @param actual Content of the `ACTUAL` file.
 */
export function tryWriteActual(path: string, actual: string) {
  try {
      fs.writeFileSync(path, actual);
  } catch (err) {
      // Likely running under Nix, ignore.
  }
}

/**
 * Truncate file paths to make test results reproducible.
 * Truncates:
 * - `file:///foo/bar/tests` -> `file:/tests`
 * - `/foo/bar/tests`        -> `/tests`
 * - `%2Ffoo%2Fbar%2Ftests`  -> `%2Ftests`
 * @param obj This object will be JSON encoded, then the paths replaced and then decoded again.
 */
export function truncatePaths(obj: any): any {
    //
    var str = JSON.stringify(obj);
    str = str.replace(/[^\": ]+\/tests/g, '/tests');
    str = str.replace(/file=(%2F|[^%])+%2Ftests/g, 'file=%2Ftests');
    return JSON.parse(str);
}

/**
 * Try to write the data as JSON to the `ACTUAL` file to disk for the expectation tests.
 * @param path path to the `ACTUAL` file.
 * @param actual Content of the `ACTUAL` file to be JSON encoded.
 */
export function tryWriteActualJSON(rootPath : string, actual : any)
{
  console.log(actual);
  tryWriteActual(rootPath + '/ACTUAL.json', JSON.stringify(actual, null, 2));
}

/**
 * Add a pretty diff to an error for mocha.
 */
export function prettyErrorJSON(e : Error, expected, actual)
{
    return Object.assign(e,
        {
            expected : JSON.stringify(expected, null, "  "),
            actual : JSON.stringify(actual, null, "  "),
            showDiff : true
        }
    );
}

export function prettyError(e : Error, expected, actual)
{
    return Object.assign(e,
        {
            expected : expected,
            actual : actual,
            showDiff : true
        }
    );
}


/**
 * Test functionalities of the DAML language server.
 * @param testDesc A description of the test.
 * @param action How should we test?
 */
export function testDaml(testDesc : string, action : DamlActionFunction)
{
    var connection: DamlConnection
    const tearDown = () => {
      connection.closeAllDocuments();
      connection.stop();
    }
    it(testDesc, function(done) {
      this.timeout(TEST_TIMEOUT);
      connection = new DamlConnection(damlCommand, ENABLE_TRACING);
      connection.start();
      connection.initialize('/tmp').then(_ => {
          action(connection)((err, subscriptions = []) => {
            tearDown();
            subscriptions.forEach((subscription) => subscription.unsubscribe())
            done(err);
          });
      }, assert.ifError);
    });
}

export function getTestDir(subdir) {
  return path.join("tests", subdir);
}
