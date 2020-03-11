// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChildProcess, spawn } from 'child_process';
import waitOn from 'wait-on';
import { encode } from 'jwt-simple';
import Ledger, { CreateEvent, ArchiveEvent, Event, Stream } from  '@daml/ledger';
import pEvent from 'p-event';

import * as buildAndLint from '@daml.js/build-and-lint-1.0.0'

const LEDGER_ID = 'build-and-lint-test';
const APPLICATION_ID = 'build-and-lint-test';
const SECRET_KEY = 'secret';
const computeToken = (party: string) => encode({
  ledgerId: LEDGER_ID,
  applicationId: APPLICATION_ID,
  party,
}, SECRET_KEY, 'HS256');

const ALICE_PARTY = 'Alice';
const ALICE_TOKEN = computeToken(ALICE_PARTY);
const BOB_PARTY = 'Bob';
const BOB_TOKEN = computeToken(BOB_PARTY);

const SANDBOX_PORT = 6865;
const JSON_API_PORT = 7575;
const HTTP_BASE_URL = `http://localhost:${JSON_API_PORT}/`;

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
  console.log ('build-and-lint-1.0.0 (' + buildAndLint.packageId + ") loaded");
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

interface PromisifiedStream<T extends object, K, I extends string, State> {
  next(): Promise<[State, readonly Event<T, K, I>[]]>;
  close(): void;
}

function promisifyStream<T extends object, K, I extends string, State>(
  stream: Stream<T, K, I, State>,
): PromisifiedStream<T, K, I, State> {
  const iterator = pEvent.iterator(stream, 'change', {rejectionEvents: ['close'], multiArgs: true});
  const next = async () => {
    const {done, value} = await iterator.next();
    expect(done).toBe(false);
    return value;
  };
  const close = () => stream.close();
  return {next, close};
}

test('create + fetch & exercise', async () => {
  const aliceLedger = new Ledger({token: ALICE_TOKEN, httpBaseUrl: HTTP_BASE_URL});
  const bobLedger = new Ledger({token: BOB_TOKEN, httpBaseUrl: HTTP_BASE_URL});
  const aliceStream = promisifyStream(aliceLedger.streamQuery(buildAndLint.Main.Person, {party: ALICE_PARTY}));
  expect(await aliceStream.next()).toEqual([[], []]);

  const alice5: buildAndLint.Main.Person = {
    name: 'Alice from Wonderland',
    party: ALICE_PARTY,
    age: '5',
    friends: [],
  };
  const bob4: buildAndLint.Main.Person = {
    name: 'Bob the Builder',
    party: BOB_PARTY,
    age: '4',
    friends: [ALICE_PARTY],
  };
  const alice5Key = {_1: alice5.party, _2: alice5.age};
  const bob4Key = {_1: bob4.party, _2: bob4.age};
  const alice5Contract = await aliceLedger.create(buildAndLint.Main.Person, alice5);
  expect(alice5Contract.payload).toEqual(alice5);
  expect(alice5Contract.key).toEqual(alice5Key);
  expect(await aliceStream.next()).toEqual([[alice5Contract], [{created: alice5Contract}]]);

  let personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([alice5Contract]);

  const aliceContracts = await aliceLedger.query(buildAndLint.Main.Person, {party: ALICE_PARTY});
  expect(aliceContracts).toEqual(personContracts);
  const bobContracts = await aliceLedger.query(buildAndLint.Main.Person, {party: BOB_PARTY});
  expect(bobContracts).toEqual([]);

  let alice5ContractById = await aliceLedger.fetch(buildAndLint.Main.Person, alice5Contract.contractId);
  expect(alice5ContractById).toEqual(alice5Contract);

  const alice5ContractByKey = await aliceLedger.fetchByKey(buildAndLint.Main.Person, alice5Key);
  expect(alice5ContractByKey).toEqual(alice5Contract);

  const bobByKey = await aliceLedger.fetchByKey(buildAndLint.Main.Person, bob4Key);
  expect(bobByKey).toBeNull();

  // Alice has a birthday and turns 6. The choice returns the new contract id.
  // There are two events: the archival of the old contract and the creation of
  // the new contract.
  let [result, events] = await aliceLedger.exercise(buildAndLint.Main.Person.Birthday, alice5Contract.contractId, {});
  expect(result).not.toEqual(alice5Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty('archived');
  expect(events[1]).toHaveProperty('created');
  const alice5Archived = (events[0] as {archived: ArchiveEvent<buildAndLint.Main.Person>}).archived;
  const alice6Contract = (events[1] as {created: CreateEvent<buildAndLint.Main.Person, buildAndLint.Main.Person.Key>}).created;
  expect(alice5Archived.contractId).toEqual(alice5Contract.contractId);
  expect(alice6Contract.contractId).toEqual(result);
  expect(alice6Contract.payload).toEqual({...alice5, age: '6'});
  expect(alice6Contract.key).toEqual({...alice5Key, _2: '6'});
  expect(await aliceStream.next()).toEqual([[alice6Contract], [{archived: alice5Archived}, {created: alice6Contract}]]);

  alice5ContractById = await aliceLedger.fetch(buildAndLint.Main.Person, alice5Contract.contractId);
  expect(alice5ContractById).toBeNull();

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([alice6Contract]);

  const alice6Key = {...alice5Key, _2: '6'};
  const alice6KeyStream = promisifyStream(aliceLedger.streamFetchByKey(buildAndLint.Main.Person, alice6Key));
  expect(await alice6KeyStream.next()).toEqual([alice6Contract, [{created: alice6Contract}]]);

  const personStream = promisifyStream(aliceLedger.streamQuery(buildAndLint.Main.Person));
  expect(await personStream.next()).toEqual([[alice6Contract], [{created: alice6Contract}]]);

  // end of non-live data, first offset
  expect(await personStream.next()).toEqual([[alice6Contract], []]);
  expect(await alice6KeyStream.next()).toEqual([alice6Contract, []]);

  // Bob enters the scene.
  const bob4Contract = await bobLedger.create(buildAndLint.Main.Person, bob4);
  expect(bob4Contract.payload).toEqual(bob4);
  expect(bob4Contract.key).toEqual(bob4Key);
  expect(await personStream.next()).toEqual([[alice6Contract, bob4Contract], [{created: bob4Contract}]]);


  // Alice changes her name.
  [result, events] = await aliceLedger.exerciseByKey(buildAndLint.Main.Person.Rename, alice6Contract.key, {newName: 'Alice Cooper'});
  expect(result).not.toEqual(alice6Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty('archived');
  expect(events[1]).toHaveProperty('created');
  const alice6Archived = (events[0] as {archived: ArchiveEvent<buildAndLint.Main.Person>}).archived;
  const cooper6Contract = (events[1] as {created: CreateEvent<buildAndLint.Main.Person, buildAndLint.Main.Person.Key>}).created;
  expect(alice6Archived.contractId).toEqual(alice6Contract.contractId);
  expect(cooper6Contract.contractId).toEqual(result);
  expect(cooper6Contract.payload).toEqual({...alice5, name: 'Alice Cooper', age: '6'});
  expect(cooper6Contract.key).toEqual(alice6Key);
  expect(await aliceStream.next()).toEqual([[cooper6Contract], [{archived: alice6Archived}, {created: cooper6Contract}]]);
  expect(await alice6KeyStream.next()).toEqual([cooper6Contract, [{archived: alice6Archived}, {created: cooper6Contract}]]);
  expect(await personStream.next()).toEqual([[bob4Contract, cooper6Contract], [{archived: alice6Archived}, {created: cooper6Contract}]]);

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([bob4Contract, cooper6Contract]);

  // Alice gets archived.
  const cooper7Archived = await aliceLedger.archiveByKey(buildAndLint.Main.Person, cooper6Contract.key);
  expect(cooper7Archived.contractId).toEqual(cooper6Contract.contractId);
  expect(await aliceStream.next()).toEqual([[], [{archived: cooper7Archived}]]);
  expect(await alice6KeyStream.next()).toEqual([null, [{archived: cooper7Archived}]]);
  expect(await personStream.next()).toEqual([[bob4Contract], [{archived: cooper7Archived}]]);

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([bob4Contract]);

  // Bob gets archived.
  const bob4Archived = await bobLedger.archive(buildAndLint.Main.Person, bob4Contract.contractId);
  expect(bob4Archived.contractId).toEqual(bob4Contract.contractId);
  expect(await personStream.next()).toEqual([[], [{archived: bob4Archived}]]);

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([]);

  aliceStream.close();
  alice6KeyStream.close();
  personStream.close();

  const allTypes: buildAndLint.Main.AllTypes = {
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
    enum: buildAndLint.Main.Color.Red,
    enumList: [buildAndLint.Main.Color.Red, buildAndLint.Main.Color.Blue, buildAndLint.Main.Color.Yellow],
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
  const allTypesContract = await aliceLedger.create(buildAndLint.Main.AllTypes, allTypes);
  expect(allTypesContract.payload).toEqual(allTypes);
  expect(allTypesContract.key).toBeUndefined();

  const allTypesContracts = await aliceLedger.query(buildAndLint.Main.AllTypes);
  expect(allTypesContracts).toEqual([allTypesContract]);

  const nonTopLevel: buildAndLint.Lib.Mod.NonTopLevel = {
    party: ALICE_PARTY,
  }
  const nonTopLevelContract = await aliceLedger.create(buildAndLint.Lib.Mod.NonTopLevel, nonTopLevel);
  expect(nonTopLevelContract.payload).toEqual(nonTopLevel);
  const nonTopLevelContracts = await aliceLedger.query(buildAndLint.Lib.Mod.NonTopLevel);
  expect(nonTopLevelContracts).toEqual([nonTopLevelContract]);
});
