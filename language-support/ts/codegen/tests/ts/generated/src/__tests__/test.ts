// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChildProcess, spawn } from 'child_process';
import waitOn from 'wait-on';
import { encode } from 'jwt-simple';
import Ledger, { CreateEvent, ArchiveEvent } from  '@digitalasset/daml-ledger-fetch';
import * as daml from '@digitalasset/daml-json-types';
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

// TODO(MH, #3518): `daml2ts` should generate this type as `Main.Person.Key`.
type PersonKey = {_1: daml.Party; _2: daml.Int}

test('create + fetch & exercise', async () => {
  const ledger = new Ledger(ALICE_TOKEN, `http://localhost:${JSON_API_PORT}/`);
  const alice5: Main.Person = {
    name: 'Alice from Wonderland',
    party: ALICE_PARTY,
    age: '5',
  };
  const alice5Key = {_1: alice5.party, _2: alice5.age};
  const alice5Contract = await ledger.create(Main.Person, alice5);
  expect(alice5Contract.payload).toEqual(alice5);
  expect(alice5Contract.key).toEqual(alice5Key);

  let personContracts = await ledger.fetchAll(Main.Person);
  expect(personContracts).toHaveLength(1);
  expect(personContracts[0]).toEqual(alice5Contract);

  const alice5ContractByKey = await ledger.lookupByKey(Main.Person, alice5Key);
  expect(alice5ContractByKey).toEqual(alice5Contract);

  const bobByKey = await ledger.lookupByKey(Main.Person, {_1: 'Bob', _2: '4'});
  expect(bobByKey).toBeNull();

  // Alice has a birthday and turns 6. The choice returns the new contract id.
  // There are two events: the archival of the old contract and the creation of
  // the new contract.
  const [result, events] = await ledger.exercise(Main.Person.Birthday, alice5Contract.contractId, {});
  expect(result).not.toEqual(alice5Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty('archived');
  expect(events[1]).toHaveProperty('created');
  const alice5Archived = (events[0] as {archived: ArchiveEvent<Main.Person>}).archived;
  const alice6Contract = (events[1] as {created: CreateEvent<Main.Person, PersonKey>}).created;
  expect(alice5Archived.contractId).toEqual(alice5Contract.contractId);
  expect(alice6Contract.contractId).toEqual(result);
  expect(alice6Contract.payload).toEqual({...alice5, age: '6'});
  expect(alice6Contract.key).toEqual({...alice5Key, _2: '6'});

  personContracts = await ledger.fetchAll(Main.Person);
  expect(personContracts).toHaveLength(1);
  expect(personContracts[0]).toEqual(alice6Contract);

  const allTypes: Main.AllTypes = {
    unit: {},
    bool: true,
    int: '5',
    text: 'Hello',
    date: '2019-04-04',
    time: '2019-12-31T12:34:56.789Z',
    party: ALICE_PARTY,
    contractId: alice5Contract.contractId,
    optional: '5',
    list: [true, false],
    textMap: {'alice': '2', 'bob & carl': '3'},
    monoRecord: alice5,
    polyRecord: {one: '10', two: 'XYZ'},
    imported: {field: {something: 'pqr'}},
    archiveX: {},
    either: {tag: 'Right', value: 'really?'},
    tuple: {_1: '12', _2: 'mmm'},
    enum: Main.Color.Red,
    enumList: [Main.Color.Red, Main.Color.Blue, Main.Color.Yellow],
    variant: {tag: 'Add', value: {_1:{tag: 'Lit', value: '1'}, _2:{tag: 'Lit', value: '2'}}},
    sumProd: {tag: 'Corge', value: {x:'1', y:'Garlpy'}}
  };
  const allTypesContract = await ledger.create(Main.AllTypes, allTypes);
  expect(allTypesContract.payload).toEqual(allTypes);
  expect(allTypesContract.key).toBeUndefined();

  const allTypesContracts = await ledger.fetchAll(Main.AllTypes);
  expect(allTypesContracts).toHaveLength(1);
  expect(allTypesContracts[0]).toEqual(allTypesContract);
});
