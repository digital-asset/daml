// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChildProcess, spawn } from 'child_process';
import waitOn from 'wait-on';
import { encode } from 'jwt-simple';
import Ledger from '@digitalasset/daml-ledger-fetch'
import * as Main from '../daml/daml-tests/Main';

const LEDGER_ID = 'daml2ts-tests';
const APPLICATION_ID = 'daml2ts-tests';
const SECRET_KEY = 'secret';
const ALICE_PARTY = 'Alice';
const ALICE_TOKEN = encode({
  ledgerId: LEDGER_ID,
  applicationId: APPLICATION_ID,
  party: ALICE_PARTY,
}, SECRET_KEY, 'HS256');

const SANDBOX_PORT = 6865;
const JSON_API_PORT = 7575;

let sandboxProcess: ChildProcess | undefined = undefined;
let jsonApiProcess: ChildProcess | undefined = undefined;

const getEnv = (variable: string): string => {
  const result = process.env[variable];
  if (!result) {
    throw Error(`${variable} not set in environment`);
  }
  return result;
}

const spawnJvmAndWaitOnPort = async (jar: string, args: string[], port: number): Promise<ChildProcess> => {
  const java = getEnv('JAVA');
  const proc = spawn(java, ['-jar', jar, ...args], {stdio: "inherit",});
  await waitOn({resources: [`tcp:localhost:${port}`]})
  return proc;
}

beforeAll(async () => {
  const darPath = getEnv('DAR');
  sandboxProcess = await spawnJvmAndWaitOnPort(
    getEnv('SANDBOX'),
    ['--port', `${SANDBOX_PORT}`, '--ledgerid', LEDGER_ID, '--wall-clock-time', darPath],
    SANDBOX_PORT,
  );
  console.log('Sandbox up');
  jsonApiProcess = await spawnJvmAndWaitOnPort(
    getEnv('JSON_API'),
    ['--ledger-host', 'localhost', '--ledger-port', `${SANDBOX_PORT}`,'--http-port', `${JSON_API_PORT}`],
    JSON_API_PORT,
  )
  console.log('JSON API up');
});

afterAll(() => {
  if (sandboxProcess) {
    sandboxProcess.kill("SIGTERM");
  }
  console.log('Killed sandbox');
  if (jsonApiProcess) {
    jsonApiProcess.kill("SIGTERM");
  }
  console.log('Killed JSON API');
});

test('create + fetch', async () => {
  const ledger = new Ledger(ALICE_TOKEN, `http://localhost:${JSON_API_PORT}/`);
  const alice: Main.Person = {
    name: 'Alice from Wonderland',
    party: ALICE_PARTY,
    age: '5',
  };
  const aliceContract = await ledger.create(Main.Person, alice);
  expect(aliceContract.argument).toEqual(alice);

  const personContracts = await ledger.fetchAll(Main.Person);
  expect(personContracts).toHaveLength(1);
  expect(personContracts[0]).toEqual(aliceContract);

  const allTypes: Main.AllTypes = {
    unit: {},
    bool: true,
    int: '5',
    text: 'Hello',
    date: '2019-04-04',
    time: '2019-12-31T12:34:56.789Z',
    party: ALICE_PARTY,
    contractId: aliceContract.contractId,
    optional: '5',
    list: [true, false],
    textMap: {'alice': '2', 'bob & carl': '3'},
    monoRecord: alice,
    polyRecord: {one: '10', two: 'XYZ'},
    imported: {field: {something: 'pqr'}},
    archiveX: {},
    either: {'Right': 'really?'},
    tuple: {_1: '12', _2: 'mmm'},
    enum: Main.Color.Red,
    enumList: [Main.Color.Red, Main.Color.Blue, Main.Color.Yellow]
  };
  const allTypesContract = await ledger.create(Main.AllTypes, allTypes);
  expect(allTypesContract.argument).toEqual(allTypes);

  const allTypesContracts = await ledger.fetchAll(Main.AllTypes);
  expect(allTypesContracts).toHaveLength(1);
  expect(allTypesContracts[0]).toEqual(allTypesContract);
});
