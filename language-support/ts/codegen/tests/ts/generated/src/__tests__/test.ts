// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChildProcess, spawn } from 'child_process';
import waitOn from 'wait-on';
import { encode } from 'jwt-simple';
import Ledger, { CreateEvent, ArchiveEvent } from  '@daml/ledger';
import pEvent from 'p-event';
import * as Main from '../daml/daml-tests/Main';
import * as LibMod from '../daml/daml-tests/Lib/Mod';

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
    ['--ledger-host', 'localhost', '--ledger-port', `${SANDBOX_PORT}`,'--http-port', `${JSON_API_PORT}`, '--websocket-config', 'heartBeatPer=1'],
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

test('create + fetch & exercise', async () => {
  const ledger = new Ledger(ALICE_TOKEN, `http://localhost:${JSON_API_PORT}/`);
  const aliceStream = ledger.streamQuery(Main.Person, {party: ALICE_PARTY});
  const aliceIterator = pEvent.iterator(aliceStream, 'events', {rejectionEvents: ['close']});
  const aliceIteratorNext = async () => {
    const {done, value} = await aliceIterator.next();
    expect(done).toBe(false);
    return value;
  };
  expect(await aliceIteratorNext()).toEqual([]);

  const alice5: Main.Person = {
    name: 'Alice from Wonderland',
    party: ALICE_PARTY,
    age: '5',
  };
  const alice5Key = {_1: alice5.party, _2: alice5.age};
  const alice5Contract = await ledger.create(Main.Person, alice5);
  expect(alice5Contract.payload).toEqual(alice5);
  expect(alice5Contract.key).toEqual(alice5Key);
  expect(await aliceIteratorNext()).toEqual([{created: alice5Contract}]);

  let personContracts = await ledger.query(Main.Person);
  expect(personContracts).toHaveLength(1);
  expect(personContracts[0]).toEqual(alice5Contract);

  const aliceContracts = await ledger.query(Main.Person, {party: ALICE_PARTY});
  expect(aliceContracts).toEqual(personContracts);
  const bobContracts = await ledger.query(Main.Person, {party: 'Bob'});
  expect(bobContracts).toEqual([]);

  let alice5ContractById = await ledger.fetch(Main.Person, alice5Contract.contractId);
  expect(alice5ContractById).toEqual(alice5Contract);

  const alice5ContractByKey = await ledger.fetchByKey(Main.Person, alice5Key);
  expect(alice5ContractByKey).toEqual(alice5Contract);

  const bobByKey = await ledger.fetchByKey(Main.Person, {_1: 'Bob', _2: '4'});
  expect(bobByKey).toBeNull();

  // Alice has a birthday and turns 6. The choice returns the new contract id.
  // There are two events: the archival of the old contract and the creation of
  // the new contract.
  let [result, events] = await ledger.exercise(Main.Person.Birthday, alice5Contract.contractId, {});
  expect(result).not.toEqual(alice5Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty('archived');
  expect(events[1]).toHaveProperty('created');
  const alice5Archived = (events[0] as {archived: ArchiveEvent<Main.Person>}).archived;
  const alice6Contract = (events[1] as {created: CreateEvent<Main.Person, Main.Person.Key>}).created;
  expect(alice5Archived.contractId).toEqual(alice5Contract.contractId);
  expect(alice6Contract.contractId).toEqual(result);
  expect(alice6Contract.payload).toEqual({...alice5, age: '6'});
  expect(alice6Contract.key).toEqual({...alice5Key, _2: '6'});
  expect(await aliceIteratorNext()).toEqual([{archived: alice5Archived}, {created: alice6Contract}]);

  alice5ContractById = await ledger.fetch(Main.Person, alice5Contract.contractId);
  expect(alice5ContractById).toBeNull();

  personContracts = await ledger.query(Main.Person);
  expect(personContracts).toHaveLength(1);
  expect(personContracts[0]).toEqual(alice6Contract);

  const alice6Key = {...alice5Key, _2: '6'};
  const alice6KeyStream = ledger.streamFetchByKey(Main.Person, alice6Key);
  const alice6KeyIterator = pEvent.iterator(alice6KeyStream, 'events', {rejectionEvents: ['close']});
  const alice6KeyIteratorNext = async () => {
    const {done, value} = await alice6KeyIterator.next();
    expect(done).toBe(false);
    return value;
  }
  expect(await alice6KeyIteratorNext()).toEqual([{created: alice6Contract}]);

  // Alice changes her name.
  [result, events] = await ledger.exerciseByKey(Main.Person.Rename, alice6Contract.key, {newName: 'Alice Cooper'});
  expect(result).not.toEqual(alice6Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty('archived');
  expect(events[1]).toHaveProperty('created');
  const alice6Archived = (events[0] as {archived: ArchiveEvent<Main.Person>}).archived;
  const cooper6Contract = (events[1] as {created: CreateEvent<Main.Person, Main.Person.Key>}).created;
  expect(alice6Archived.contractId).toEqual(alice6Contract.contractId);
  expect(cooper6Contract.contractId).toEqual(result);
  expect(cooper6Contract.payload).toEqual({...alice5, name: 'Alice Cooper', age: '6'});
  expect(cooper6Contract.key).toEqual(alice6Key);
  expect(await aliceIteratorNext()).toEqual([{archived: alice6Archived}, {created: cooper6Contract}]);
  expect(await alice6KeyIteratorNext()).toEqual([{archived: alice6Archived}, {created: cooper6Contract}]);

  personContracts = await ledger.query(Main.Person);
  expect(personContracts).toHaveLength(1);
  expect(personContracts[0]).toEqual(cooper6Contract);

  // Alice gets archived.
  const cooper7Archived = await ledger.archiveByKey(Main.Person, cooper6Contract.key);
  expect(cooper7Archived.contractId).toEqual(cooper6Contract.contractId);
  expect(await aliceIteratorNext()).toEqual([{archived: cooper7Archived}]);
  expect(await alice6KeyIteratorNext()).toEqual([{archived: cooper7Archived}]);

  personContracts = await ledger.query(Main.Person);
  expect(personContracts).toHaveLength(0);

  aliceStream.close();
  alice6KeyStream.close();

  const allTypes: Main.AllTypes = {
    unit: {},
    bool: true,
    int: '5',
    text: 'Hello',
    date: '2019-04-04',
    time: '2019-12-31T12:34:56.789Z',
    party: ALICE_PARTY,
    contractId: alice5Contract.contractId,
    optional: '5', // Some 5
    optional2: null,  // None
    optionalOptionalInt: ['5'], // Some (Some 5)
    optionalOptionalInt2: [], // Some (None)
    optionalOptionalInt3: null, // None
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
    optionalVariant: {tag: 'Add', value: {_1:{tag: 'Lit', value: '1'}, _2:{tag: 'Lit', value: '2'}}},
    sumProd: {tag: 'Corge', value: {x:'1', y:'Garlpy'}},
    optionalSumProd: {tag: 'Corge', value: {x:'1', y:'Garlpy'}},
    parametericSumProd: {tag: 'Add2', value: {lhs:{tag: 'Lit2', value: '1'}, rhs:{tag: 'Lit2', value: '2'}}},
    optionalOptionalParametericSumProd: [{tag: 'Add2', value: {lhs:{tag: 'Lit2', value: '1'}, rhs:{tag: 'Lit2', value: '2'}}}],
    n0:  '3.0',          // Numeric 0
    n5:  '3.14159',      // Numeric 5
    n10: '3.1415926536', // Numeric 10
  };
  const allTypesContract = await ledger.create(Main.AllTypes, allTypes);
  expect(allTypesContract.payload).toEqual(allTypes);
  expect(allTypesContract.key).toBeUndefined();

  const allTypesContracts = await ledger.query(Main.AllTypes);
  expect(allTypesContracts).toHaveLength(1);
  expect(allTypesContracts[0]).toEqual(allTypesContract);

  const NonTopLevel: LibMod.NonTopLevel = {
    party: ALICE_PARTY,
  }
  const NonTopLevelContract = await ledger.create(LibMod.NonTopLevel, NonTopLevel);
  expect(NonTopLevelContract.payload).toEqual(NonTopLevel);
  const NonTopLevelContracts = await ledger.query(LibMod.NonTopLevel);
  expect(NonTopLevelContracts).toHaveLength(1);
  expect(NonTopLevelContracts[0]).toEqual(NonTopLevelContract);
});
