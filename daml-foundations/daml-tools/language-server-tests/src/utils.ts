// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

'use strict';


// Utility functions for writing unit tests for the DAML language server.

import * as assert from 'assert';
import * as fs from 'fs';
import { inspect } from 'util'
import Uri from 'vscode-uri';

export function getDirectories(baseDir: string): string[] {
  return fs.readdirSync(baseDir)
    .map(name => baseDir + '/' + name)
    .filter(path => fs.statSync(path).isDirectory());
}

/**
 * Read a file from disk into memory as string.
 * @param path Path to the file on disk.
 */
export function readFileAsString(path: string): string
{
  return <string> fs.readFileSync(path, { encoding: "utf8" });
}

/**
 * Filter duplicate values in array
 * @param arr The array to filter.
 */
export function unique<T>(arr : Array<T>)
{
  return Array.from(new Set(arr));
}

/**
 * Pretty-prints JS objects for node. Console.log in node doesn't handle nested objects.
 * @param val The object to print.
 * @param descr An optional description of the object.
 */
export function prettyLog(val : any, descr : string = "")
{
  if(descr !== "")
  {
    console.log(descr)
  }
  console.log(inspect(val, { colors : true, depth: null }));
}

/**
 * Result of a deep equality check.
 */
export interface DeepEqualResult {
    /**
     * Are the values equal?
     */
    equal : boolean
    /**
     * If the values are not equal, tell me why.
     */
    message : string
}

/**
 * Test two values for deep equality.
 * @param actual
 * @param expected
 */
export function deepEqual(actual, expected, message? : string) : DeepEqualResult
{
  try {
    assert.deepEqual(actual, expected, message);
    return { equal: true, message: "" };
  }
  catch(err) {
    return { equal:false , message: err };
  }
}

export function toRequestUri(u : string) : string {
  return Uri.file(u).toString();
}
