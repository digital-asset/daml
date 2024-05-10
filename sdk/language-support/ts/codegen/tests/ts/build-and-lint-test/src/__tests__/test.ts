// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChildProcess, execFileSync, spawn } from "child_process";
import { promises as fs } from "fs";
import waitOn from "wait-on";
import { encode } from "jwt-simple";
import Ledger, {
  CreateEvent,
  Event,
  Stream,
  PartyInfo,
  Query,
  UserRightHelper,
} from "@daml/ledger";
import {
  Choice,
  ContractId,
  Int,
  emptyMap,
  Map,
  Party,
  Template,
  lookupTemplate,
} from "@daml/types";
import pEvent from "p-event";
import _ from "lodash";
import WebSocket from "ws";

import * as buildAndLint from "@daml.js/build-and-lint-1.0.0";

const LEDGER_ID = "build-and-lint-test";
const APPLICATION_ID = "build-and-lint-test";
const SECRET_KEY = "secret";
const computeToken = (party: string) =>
  encode(
    {
      "https://daml.com/ledger-api": {
        ledgerId: LEDGER_ID,
        applicationId: APPLICATION_ID,
        actAs: [party],
      },
    },
    SECRET_KEY,
    "HS256",
  );

const computeUserToken = (name: string) =>
  encode(
    {
      sub: name,
      scope: "daml_ledger_api",
    },
    SECRET_KEY,
    "HS256",
  );

const ADMIN_TOKEN = encode(
  {
    "https://daml.com/ledger-api": {
      ledgerId: LEDGER_ID,
      applicationId: APPLICATION_ID,
      admin: true,
    },
  },
  SECRET_KEY,
  "HS256",
);
// Further setup required after canton is started
let ALICE_PARTY = "Alice";
let ALICE_TOKEN = "";
let BOB_PARTY = "Bob";
let BOB_TOKEN = "";
let CHARLIE_PARTY = "Charlie";
let CHARLIE_TOKEN = "";
// Will be `build-and-lint-test::[somehash]`
let PARTICIPANT_PARTY_DETAILS: PartyInfo | undefined = undefined;

let sandboxPort: number | undefined = undefined;
const SANDBOX_PORT_FILE = "sandbox.port";
let jsonApiPort: number | undefined = undefined;
const JSON_API_PORT_FILE = "json-api.port";
const httpBaseUrl: () => string = () => `http://localhost:${jsonApiPort}/`;

let sandboxProcess: ChildProcess | undefined = undefined;
let jsonApiProcess: ChildProcess | undefined = undefined;

const getEnv = (variable: string): string => {
  const result = process.env[variable];
  if (!result) {
    throw Error(`${variable} not set in environment`);
  }
  return result;
};

const spawnJvm = (
  jar: string,
  args: string[],
  jvmArgs: string[] = [],
): ChildProcess => {
  const java = getEnv("JAVA");
  const proc = spawn(java, [...jvmArgs, "-jar", jar, ...args], {
    stdio: "inherit",
  });
  return proc;
};

beforeAll(async () => {
  console.log("build-and-lint-1.0.0 (" + buildAndLint.packageId + ") loaded");
  sandboxProcess = spawnJvm(getEnv("CANTON"), [
    "daemon",
    "-c",
    "./src/__tests__/canton.conf",
    "-C",
    "canton.parameters.ports-file=" + SANDBOX_PORT_FILE,
    "--auto-connect-local",
  ]);
  await waitOn({ resources: [`file:${SANDBOX_PORT_FILE}`] });
  const sandboxPortData = await fs.readFile(SANDBOX_PORT_FILE, {
    encoding: "utf8",
  });

  const sandboxPortJson = JSON.parse(sandboxPortData);
  sandboxPort = sandboxPortJson["build-and-lint-test"].ledgerApi;
  if (!sandboxPort) throw "Invalid port file";
  console.log("Sandbox listening on port " + sandboxPort.toString());

  jsonApiProcess = spawnJvm(
    getEnv("JSON_API"),
    [
      "--ledger-host",
      "localhost",
      "--ledger-port",
      `${sandboxPort}`,
      "--port-file",
      JSON_API_PORT_FILE,
      "--http-port",
      "0",
      "--allow-insecure-tokens",
      "--websocket-config=maxDuration=1,heartBeatPer=1",
      "--log-level=INFO",
    ],
    ["-Dpekko.http.server.request-timeout=60s"],
  );
  await waitOn({ resources: [`file:${JSON_API_PORT_FILE}`] });
  const jsonApiPortData = await fs.readFile(JSON_API_PORT_FILE, {
    encoding: "utf8",
  });
  jsonApiPort = parseInt(jsonApiPortData);

  console.log("Uploading required dar files ..." + getEnv("DAR"));
  const ledger = new Ledger({ token: ADMIN_TOKEN, httpBaseUrl: httpBaseUrl() });
  const upDar = await fs.readFile(getEnv("DAR"));
  await ledger.uploadDarFile(upDar);

  // Only the participant party should exist on the ledger at this point
  PARTICIPANT_PARTY_DETAILS = (await ledger.listKnownParties())[0];

  console.log("Explicitly allocating parties");
  ALICE_PARTY = (
    await ledger.allocateParty({
      identifierHint: ALICE_PARTY,
      displayName: ALICE_PARTY,
    })
  ).identifier;
  BOB_PARTY = (
    await ledger.allocateParty({
      identifierHint: BOB_PARTY,
      displayName: BOB_PARTY,
    })
  ).identifier;
  CHARLIE_PARTY = (
    await ledger.allocateParty({
      identifierHint: CHARLIE_PARTY,
      displayName: CHARLIE_PARTY,
    })
  ).identifier;

  ALICE_TOKEN = computeToken(ALICE_PARTY);
  BOB_TOKEN = computeToken(BOB_PARTY);
  CHARLIE_TOKEN = computeToken(CHARLIE_PARTY);

  console.log("JSON API listening on port " + jsonApiPort.toString());
}, 300_000);

afterAll(() => {
  if (sandboxProcess) {
    sandboxProcess.kill("SIGTERM");
  }
  console.log("Killed sandbox");
  if (jsonApiProcess) {
    jsonApiProcess.kill("SIGTERM");
  }
  console.log("Killed JSON API");
});

interface PromisifiedStream<T extends object, K, I extends string, State> {
  next(): Promise<[State, readonly Event<T, K, I>[]]>;
  close(): void;
}

function promisifyStream<T extends object, K, I extends string, State>(
  stream: Stream<T, K, I, State>,
): PromisifiedStream<T, K, I, State> {
  const iterator = pEvent.iterator(stream, "change", {
    rejectionEvents: ["close"],
    multiArgs: true,
  });
  const next = async () => {
    const { done, value } = await iterator.next();
    expect(done).toBe(false);
    return value;
  };
  const close = () => stream.close();
  return { next, close };
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe("decoders for recursive types do not loop", () => {
  test("recursive enum", () => {
    expect(buildAndLint.Main.Expr(Int).decoder.run(undefined).ok).toBe(false);
  });

  test("recursive record with guards", () => {
    expect(buildAndLint.Main.Recursive.decoder.run(undefined).ok).toBe(false);
  });

  test("uninhabited record", () => {
    expect(buildAndLint.Main.VoidRecord.decoder.run(undefined).ok).toBe(false);
  });

  test("uninhabited enum", () => {
    expect(buildAndLint.Main.VoidEnum.decoder.run(undefined).ok).toBe(false);
  });
});

test("create + fetch & exercise", async () => {
  const aliceLedger = new Ledger({
    token: ALICE_TOKEN,
    httpBaseUrl: httpBaseUrl(),
  });
  const bobLedger = new Ledger({
    token: BOB_TOKEN,
    httpBaseUrl: httpBaseUrl(),
  });
  const aliceRawStream = aliceLedger.streamQuery(buildAndLint.Main.Person, {
    party: ALICE_PARTY,
  });
  const aliceStream = promisifyStream(aliceRawStream);
  // TODO(MH): Move this live marker into `promisifyStream`. Unfortunately,
  // it didn't work the straightforward way and we need to spend more time
  // figuring out what's going wrong before we can do it. There are two more
  // instances of this pattern below.
  const aliceStreamLive = pEvent(aliceRawStream, "live");
  expect(await aliceStreamLive).toEqual([]);

  const alice5: buildAndLint.Main.Person = {
    name: "Alice from Wonderland",
    party: ALICE_PARTY,
    age: "5",
    friends: [],
  };
  const bob4: buildAndLint.Main.Person = {
    name: "Bob the Builder",
    party: BOB_PARTY,
    age: "4",
    friends: [ALICE_PARTY],
  };
  const alice5Key = { _1: alice5.party, _2: alice5.age };
  const bob4Key = { _1: bob4.party, _2: bob4.age };
  const alice5Contract = await aliceLedger.create(
    buildAndLint.Main.Person,
    alice5,
  );
  expect(alice5Contract.payload).toEqual(alice5);
  expect(alice5Contract.key).toEqual(alice5Key);
  expect(await aliceStream.next()).toEqual([
    [alice5Contract],
    [{ created: alice5Contract, matchedQueries: [0] }],
  ]);

  let personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([alice5Contract]);

  const aliceContracts = await aliceLedger.query(buildAndLint.Main.Person, {
    party: ALICE_PARTY,
  });
  expect(aliceContracts).toEqual(personContracts);
  const bobContracts = await aliceLedger.query(buildAndLint.Main.Person, {
    party: BOB_PARTY,
  });
  expect(bobContracts).toEqual([]);

  let alice5ContractById = await aliceLedger.fetch(
    buildAndLint.Main.Person,
    alice5Contract.contractId,
  );
  expect(alice5ContractById).toEqual(alice5Contract);

  const alice5ContractByKey = await aliceLedger.fetchByKey(
    buildAndLint.Main.Person,
    alice5Key,
  );
  expect(alice5ContractByKey).toEqual(alice5Contract);

  const bobByKey = await aliceLedger.fetchByKey(
    buildAndLint.Main.Person,
    bob4Key,
  );
  expect(bobByKey).toBeNull();

  // Alice has a birthday and turns 6. The choice returns the new contract id.
  // There are two events: the archival of the old contract and the creation of
  // the new contract.
  let [result, events] = await aliceLedger.exercise(
    buildAndLint.Main.Person.Birthday,
    alice5Contract.contractId,
    {},
  );
  expect(result).not.toEqual(alice5Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty("archived");
  expect(events[1]).toHaveProperty("created");
  const alice5Archived = (
    events[0] as { archived: buildAndLint.Main.Person.ArchiveEvent }
  ).archived;
  const alice6Contract = (
    events[1] as { created: buildAndLint.Main.Person.CreateEvent }
  ).created;
  expect(alice5Archived.contractId).toEqual(alice5Contract.contractId);
  expect(alice6Contract.contractId).toEqual(result);
  expect(alice6Contract.payload).toEqual({ ...alice5, age: "6" });
  expect(alice6Contract.key).toEqual({ ...alice5Key, _2: "6" });
  expect(await aliceStream.next()).toEqual([
    [alice6Contract],
    [
      { archived: alice5Archived },
      { created: alice6Contract, matchedQueries: [0] },
    ],
  ]);

  alice5ContractById = await aliceLedger.fetch(
    buildAndLint.Main.Person,
    alice5Contract.contractId,
  );
  expect(alice5ContractById).toBeNull();

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([alice6Contract]);

  const alice6Key = { ...alice5Key, _2: "6" };
  const alice6KeyRawStream = aliceLedger.streamFetchByKey(
    buildAndLint.Main.Person,
    alice6Key,
  );
  const alice6KeyStream = promisifyStream(alice6KeyRawStream);
  const alice6KeyStreamLive = pEvent(alice6KeyRawStream, "live");
  expect(await alice6KeyStream.next()).toEqual([
    alice6Contract,
    [{ created: alice6Contract }],
  ]);

  const personRawStream = aliceLedger.streamQuery(buildAndLint.Main.Person);
  const personStream = promisifyStream(personRawStream);
  const personStreamLive = pEvent(personRawStream, "live");
  expect(await personStream.next()).toEqual([
    [alice6Contract],
    [{ created: alice6Contract, matchedQueries: [0] }],
  ]);

  // end of non-live data, first offset
  expect(await personStreamLive).toEqual([alice6Contract]);
  expect(await alice6KeyStreamLive).toEqual(alice6Contract);

  // Bob enters the scene.
  const bob4Contract = await bobLedger.create(buildAndLint.Main.Person, bob4);
  expect(bob4Contract.payload).toEqual(bob4);
  expect(bob4Contract.key).toEqual(bob4Key);
  expect(await personStream.next()).toEqual([
    [alice6Contract, bob4Contract],
    [{ created: bob4Contract, matchedQueries: [0] }],
  ]);

  // Alice changes her name.
  [result, events] = await aliceLedger.exerciseByKey(
    buildAndLint.Main.Person.Rename,
    alice6Contract.key,
    { newName: "Alice Cooper" },
  );
  expect(result).not.toEqual(alice6Contract.contractId);
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty("archived");
  expect(events[1]).toHaveProperty("created");
  const alice6Archived = (
    events[0] as { archived: buildAndLint.Main.Person.ArchiveEvent }
  ).archived;
  const cooper6Contract = (
    events[1] as { created: buildAndLint.Main.Person.CreateEvent }
  ).created;
  expect(alice6Archived.contractId).toEqual(alice6Contract.contractId);
  expect(cooper6Contract.contractId).toEqual(result);
  expect(cooper6Contract.payload).toEqual({
    ...alice5,
    name: "Alice Cooper",
    age: "6",
  });
  expect(cooper6Contract.key).toEqual(alice6Key);
  expect(await aliceStream.next()).toEqual([
    [cooper6Contract],
    [
      { archived: alice6Archived },
      { created: cooper6Contract, matchedQueries: [0] },
    ],
  ]);
  expect(await alice6KeyStream.next()).toEqual([
    cooper6Contract,
    [{ archived: alice6Archived }, { created: cooper6Contract }],
  ]);
  expect(await personStream.next()).toEqual([
    [bob4Contract, cooper6Contract],
    [
      { archived: alice6Archived },
      { created: cooper6Contract, matchedQueries: [0] },
    ],
  ]);

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([bob4Contract, cooper6Contract]);

  // Alice gets archived.
  const cooper7Archived = await aliceLedger.archiveByKey(
    buildAndLint.Main.Person,
    cooper6Contract.key,
  );
  expect(cooper7Archived.contractId).toEqual(cooper6Contract.contractId);
  expect(await aliceStream.next()).toEqual([
    [],
    [{ archived: cooper7Archived }],
  ]);
  expect(await alice6KeyStream.next()).toEqual([
    null,
    [{ archived: cooper7Archived }],
  ]);
  expect(await personStream.next()).toEqual([
    [bob4Contract],
    [{ archived: cooper7Archived }],
  ]);

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([bob4Contract]);

  // Bob gets archived.
  const bob4Archived = await bobLedger.archive(
    buildAndLint.Main.Person,
    bob4Contract.contractId,
  );
  expect(bob4Archived.contractId).toEqual(bob4Contract.contractId);
  expect(await personStream.next()).toEqual([[], [{ archived: bob4Archived }]]);

  personContracts = await aliceLedger.query(buildAndLint.Main.Person);
  expect(personContracts).toEqual([]);

  aliceStream.close();
  alice6KeyStream.close();
  personStream.close();

  const map: Map<buildAndLint.Main.Expr2<Int>, Int> = emptyMap<
    buildAndLint.Main.Expr2<Int>,
    Int
  >()
    .set(
      {
        tag: "Add2",
        value: {
          lhs: { tag: "Lit2", value: "1" },
          rhs: { tag: "Lit2", value: "2" },
        },
      },
      "3",
    )
    .set(
      {
        tag: "Add2",
        value: {
          lhs: { tag: "Lit2", value: "5" },
          rhs: { tag: "Lit2", value: "4" },
        },
      },
      "9",
    )
    .set(
      {
        tag: "Add2",
        value: {
          lhs: { tag: "Lit2", value: "2" },
          rhs: { tag: "Lit2", value: "1" },
        },
      },
      "3",
    )
    .set(
      {
        tag: "Add2",
        value: {
          lhs: { tag: "Lit2", value: "3" },
          rhs: { tag: "Lit2", value: "1" },
        },
      },
      "4",
    );
  const allTypes: buildAndLint.Main.AllTypes = {
    unit: {},
    bool: true,
    int: "5",
    text: "Hello",
    date: "2019-04-04",
    time: "2019-12-31T12:34:56.789Z",
    party: ALICE_PARTY,
    contractId: alice5Contract.contractId,
    optional: "5", // Some 5
    optional2: null, // None
    optionalOptionalInt: ["5"], // Some (Some 5)
    optionalOptionalInt2: [], // Some (None)
    optionalOptionalInt3: null, // None
    list: [true, false],
    textMap: { alice: "2", "bob & carl": "3" },
    monoRecord: alice5,
    polyRecord: { one: "10", two: "XYZ" },
    imported: { field: { something: "pqr" } },
    archiveX: {},
    either: { tag: "Right", value: "really?" },
    tuple: { _1: "12", _2: "mmm" },
    enum: buildAndLint.Main.Color.Red,
    enumList: [
      buildAndLint.Main.Color.Red,
      buildAndLint.Main.Color.Blue,
      buildAndLint.Main.Color.Yellow,
    ],
    enumList2: ["Red", "Blue", "Yellow"],
    optcol1: { tag: "Transparent1", value: {} },
    optcol2: { tag: "Color2", value: { color2: "Red" } }, // 'Red' is of type Color
    optcol3: { tag: "Color2", value: { color2: buildAndLint.Main.Color.Blue } }, // Color.Blue is of type 'Color'
    variant: {
      tag: "Add",
      value: { _1: { tag: "Lit", value: "1" }, _2: { tag: "Lit", value: "2" } },
    },
    optionalVariant: {
      tag: "Add",
      value: { _1: { tag: "Lit", value: "1" }, _2: { tag: "Lit", value: "2" } },
    },
    sumProd: { tag: "Corge", value: { x: "1", y: "Garlpy" } },
    optionalSumProd: { tag: "Corge", value: { x: "1", y: "Garlpy" } },
    parametericSumProd: {
      tag: "Add2",
      value: {
        lhs: { tag: "Lit2", value: "1" },
        rhs: { tag: "Lit2", value: "2" },
      },
    },
    optionalOptionalParametericSumProd: [
      {
        tag: "Add2",
        value: {
          lhs: { tag: "Lit2", value: "1" },
          rhs: { tag: "Lit2", value: "2" },
        },
      },
    ],
    n0: "3.0", // Numeric 0
    n5: "3.14159", // Numeric 5
    n10: "3.1415926536", // Numeric 10
    rec: { recOptional: null, recList: [], recTextMap: {} },
    voidRecord: null,
    voidEnum: null,
    genMap: map,
  };
  const allTypesContract = await aliceLedger.create(
    buildAndLint.Main.AllTypes,
    allTypes,
  );
  expect(allTypesContract.payload).toEqual(allTypes);
  expect(allTypesContract.key).toBeUndefined();

  const allTypesContracts = await aliceLedger.query(buildAndLint.Main.AllTypes);
  expect(allTypesContracts).toEqual([allTypesContract]);

  const nonTopLevel: buildAndLint.Lib.Mod.NonTopLevel = {
    party: ALICE_PARTY,
  };
  const nonTopLevelContract = await aliceLedger.create(
    buildAndLint.Lib.Mod.NonTopLevel,
    nonTopLevel,
  );
  expect(nonTopLevelContract.payload).toEqual(nonTopLevel);
  const nonTopLevelContracts = await aliceLedger.query(
    buildAndLint.Lib.Mod.NonTopLevel,
  );
  expect(nonTopLevelContracts).toEqual([nonTopLevelContract]);
});

test("exercise using explicit disclosure", async () => {
  const aliceLedger = new Ledger({
    token: ALICE_TOKEN,
    httpBaseUrl: httpBaseUrl(),
  });
  const bobLedger = new Ledger({
    token: BOB_TOKEN,
    httpBaseUrl: httpBaseUrl(),
  });

  const payload: buildAndLint.Main.ReferenceData = {
    p: ALICE_PARTY,
  };
  const contract = await aliceLedger.create(
    buildAndLint.Main.ReferenceData,
    payload,
  );
  // TODO(https://digitalasset.atlassian.net/browse/LT-5)
  // The JSON API does not expose created_event_blob so we read it directly through the gRPC API.
  const output = execFileSync(
    "grpcurl",
    [
      "-plaintext",
      "-H",
      `Authorization: Bearer ${ALICE_TOKEN}`,
      "-d",
      JSON.stringify({
        filter: {
          filtersByParty: {
            [ALICE_PARTY]: {
              inclusive: {
                templateFilters: [
                  {
                    templateId: {
                      packageId: buildAndLint.packageId,
                      moduleName: "Main",
                      entityName: "ReferenceData",
                    },
                    includeCreatedEventBlob: true,
                  },
                ],
              },
            },
          },
        },
        begin: { boundary: "LEDGER_BEGIN" },
        end: { boundary: "LEDGER_END" },
      }),
      "localhost:5011",
      "com.daml.ledger.api.v1.TransactionService/GetTransactions",
    ],
    { encoding: "utf8" },
  );
  const created = JSON.parse(output).transactions[0].events[0].created;
  const [result] = await bobLedger.exercise(
    buildAndLint.Main.ReferenceData.ReferenceData_Fetch,
    contract.contractId,
    { fetcher: BOB_PARTY },
    {
      disclosedContracts: [
        {
          contractId: contract.contractId,
          templateId: contract.templateId,
          createdEventBlob: created.createdEventBlob,
        },
      ],
    },
  );
  expect(result).toEqual(payload);
});

describe("interface definition", () => {
  const tpl = buildAndLint.Main.Asset;
  const if1 = buildAndLint.Main.Token;
  const if2 = buildAndLint.Lib.Mod.Other;
  test("separate object from template", () => {
    expect(if1).not.toBe(tpl);
    expect(if2).not.toBe(tpl);
  });
  test("template IDs not overwritten", () => {
    expect(if1.templateId).not.toEqual(tpl.templateId);
    expect(if2.templateId).not.toEqual(tpl.templateId);
  });
  test("choices not copied to interfaces", () => {
    const key1 = "Transfer";
    const key2 = "Something";
    expect(if1).toHaveProperty(key1);
    expect(if2).toHaveProperty(key2);
    expect(if1).not.toHaveProperty(key2);
    expect(if2).not.toHaveProperty(key1);
  });
  test("even with no choices", () => {
    const emptyIfc = buildAndLint.Lib.EmptyIfaceOnly.NoChoices;
    const emptyIfcId: string = emptyIfc.templateId;
    expect(emptyIfc).toMatchObject({
      templateId: emptyIfcId,
    });
  });
  describe("choice name collision", () => {
    // statically assert that an expression is a choice
    const theChoice = <T extends object, C, R, K>(c: Choice<T, C, R, K>) => c;

    test("choice from two interfaces is not inherited", () => {
      const k = "PeerIfaceOverload";
      expect(theChoice(if2[k])).toBeDefined();
      expect(
        theChoice(buildAndLint.Lib.ModIfaceOnly.YetAnother[k]),
      ).toBeDefined();
      // statically check that k isn't in tpl
      const tplK: Extract<keyof typeof tpl, typeof k> extends never
        ? true
        : never = true;
      expect(tplK).toEqual(true); // useless, but suppresses unused error
      // dynamically check the same
      expect(_.get(tpl, k)).toBeUndefined();
    });
    test("choice from template and interface prefers template", () => {
      const k = "Overridden";
      const c: Choice<
        buildAndLint.Main.Asset,
        buildAndLint.Main.Overridden,
        {},
        undefined
      > = tpl[k];
      expect(c).not.toEqual(theChoice(if2[k]));
      expect(c.template()).toBe(tpl);
    });
  });

  test("retroactive interfaces permit contract ID conversion", () => {
    const cid = "test" as ContractId<buildAndLint.Main.Asset>;
    const icid: ContractId<buildAndLint.Retro.Retro> = tpl.toInterface(
      buildAndLint.Retro.Retro,
      cid,
    );
    const tcid: ContractId<buildAndLint.Main.Asset> = tpl.unsafeFromInterface(
      buildAndLint.Retro.Retro,
      icid,
    );
    expect(icid).toBe(cid);
    expect(tcid).toBe(icid);
  });
});

describe("interfaces", () => {
  type Asset = buildAndLint.Main.Asset;
  const Asset = buildAndLint.Main.Asset;
  type Token = buildAndLint.Main.Token;
  const Token = buildAndLint.Main.Token;

  type RecPartial<T> = T extends object
    ? {
        [P in keyof T]?: RecPartial<T[P]>;
      }
    : T;

  async function aliceLedgerPayloadContract() {
    const aliceLedger = new Ledger({
      token: ALICE_TOKEN,
      httpBaseUrl: httpBaseUrl(),
    });
    const assetPayload = {
      issuer: ALICE_PARTY,
      owner: ALICE_PARTY,
    };
    const expectedView: buildAndLint.Main.TokenView = {
      tokenOwner: ALICE_PARTY,
    };
    return {
      aliceLedger,
      assetPayload,
      expectedView: expectedView as Token,
      contract: await aliceLedger.create(Asset, assetPayload),
    };
  }

  test("interface companion choice exercise", async () => {
    const {
      aliceLedger,
      assetPayload,
      contract: ifaceContract,
    } = await aliceLedgerPayloadContract();

    expect(ifaceContract.payload).toEqual(assetPayload);
    const [, events1] = await aliceLedger.exercise(
      Token.Transfer,
      Asset.toInterface(Token, ifaceContract.contractId),
      { newOwner: BOB_PARTY },
    );
    expect(events1).toMatchObject([
      { archived: { templateId: buildAndLint.Main.Asset.templateId } },
      {
        created: {
          templateId: buildAndLint.Main.Asset.templateId,
          signatories: [ALICE_PARTY],
          payload: { issuer: ALICE_PARTY, owner: BOB_PARTY },
        },
      },
    ]);
  });

  test("sync query without predicate", async () => {
    const { aliceLedger, expectedView, contract } =
      await aliceLedgerPayloadContract();
    const acs = await aliceLedger.query(Token);
    const expectedAc: typeof acs extends (infer ce)[]
      ? Omit<ce, "key">
      : never = {
      templateId: Token.templateId, // NB: the interface ID
      contractId: Asset.toInterface(Token, contract.contractId),
      signatories: [ALICE_PARTY],
      observers: [],
      agreementText: "",
      payload: expectedView,
    };
    expect(acs).toContainEqual(expectedAc);
  });

  test("sync query with predicate", async () => {
    const { aliceLedger, expectedView, contract } =
      await aliceLedgerPayloadContract();
    function isCt(ev: CreateEvent<Token>) {
      return ev.contractId === Asset.toInterface(Token, contract.contractId);
    }
    // check that Query type accepts removing the brand
    const succeedingQuery: Query<Token> = {
      tokenOwner: ALICE_PARTY,
    };
    const failingQuery = { tokenOwner: BOB_PARTY };

    const foundCt = (await aliceLedger.query(Token, succeedingQuery)).filter(
      isCt,
    );
    const noCt = (await aliceLedger.query(Token, failingQuery)).filter(isCt);
    const expectedMatchedContract = {
      payload: expectedView,
      templateId: Token.templateId,
    };
    expect(foundCt).toMatchObject([expectedMatchedContract]);
    expect(noCt).toEqual([]);
  });

  test("fetch", async () => {
    const { aliceLedger, expectedView, contract } =
      await aliceLedgerPayloadContract();
    const fetched = await aliceLedger.fetch(
      Token,
      Asset.toInterface(Token, contract.contractId),
    );
    expect(fetched).toMatchObject({
      contractId: contract.contractId,
      templateId: Token.templateId,
      payload: expectedView,
    });
  });

  test("WS query", async () => {
    const { aliceLedger, expectedView, contract } =
      await aliceLedgerPayloadContract();
    const tokenCid = Asset.toInterface(Token, contract.contractId);
    const stream = aliceLedger.streamQueries(Token, [expectedView]);
    type FoundCreate = typeof stream extends Stream<
      object,
      unknown,
      string,
      (infer ce)[]
    >
      ? { created: ce }
      : never;
    function isCreateForContract(ev: Event<Token>): ev is FoundCreate {
      return "created" in ev && ev.created.contractId === tokenCid;
    }
    function queryContractId(): Promise<FoundCreate> {
      let found = false;
      return new Promise((resolve, reject) => {
        stream.on("change", (_, evs) => {
          if (found) return;
          const ev = evs.find<FoundCreate>(isCreateForContract);
          if (ev) {
            found = true;
            resolve(ev);
          }
        });
        stream.on("close", closeEv => found || reject(closeEv.reason));
      });
    }
    const ctEvent = await queryContractId();
    stream.close();
    const expectedEvent: RecPartial<Event<Token>> = {
      created: {
        contractId: tokenCid,
        templateId: Token.templateId,
        payload: expectedView,
      },
      matchedQueries: [0],
    };
    expect(ctEvent).toMatchObject(expectedEvent);
  });

  test("undecodable exercise result event data is discarded", async () => {
    const ledger = new Ledger({
      token: ALICE_TOKEN,
      httpBaseUrl: httpBaseUrl(),
    });
    // upload a dar we did not codegen
    const hiddenDar = await fs.readFile(getEnv("HIDDEN_DAR"));
    await ledger.uploadDarFile(hiddenDar);

    // Wait for the domain to see the dar
    // TODO[SW]: Find a way to do this by asking the domain
    await new Promise(resolve => setTimeout(resolve, 1000));

    // pretend we have access to NotVisibleInTs.  For this test to be
    // meaningful we *must not* codegen or load Hidden
    type NotVisibleInTs = { owner: Party };
    const NotVisibleInTs: Pick<Template<{ owner: Party }>, "templateId"> = {
      templateId: "#hidden:Hidden:NotVisibleInTs",
    };
    const initialPayload: NotVisibleInTs = { owner: ALICE_PARTY };

    // make a contract whose template is not in the JS image
    // we can't use ledger.create without knowing the exact template ID
    const submittableLedger = ledger as unknown as {
      submit: (typeof ledger)["submit"];
    };
    const {
      templateId: nvitFQTID,
      contractId: nvitCid,
      payload: payloadResp,
      key: keyResp,
    } = (await submittableLedger.submit("v1/create", {
      templateId: NotVisibleInTs.templateId,
      payload: initialPayload,
    })) as CreateEvent<NotVisibleInTs, { _1: Text; _2: Party }>;
    expect(nvitFQTID).toEqual(expect.stringMatching(/:Hidden:NotVisibleInTs$/));
    expect(payloadResp).toEqual(initialPayload);
    expect(keyResp).toEqual({ _1: "three three three", _2: ALICE_PARTY });
    // verify that we don't know the decoder for NotVisibleInTs
    expect(() => lookupTemplate(nvitFQTID)).toThrow();

    const nvitIcid: ContractId<buildAndLint.Main.Cloneable> =
      nvitCid as ContractId<never>;

    // invoke well-typed interface choice
    const [{ _1: newIcid, _2: textResponse }, archiveAndCreate] =
      await ledger.exercise(buildAndLint.Main.Cloneable.Clone, nvitIcid, {
        echo: "undecodable exercise result test",
      });
    const expectedEvents: RecPartial<Event<object>>[] = [
      { archived: { contractId: nvitCid, templateId: nvitFQTID } },
      { created: { contractId: newIcid, templateId: nvitFQTID } },
    ];

    // test events are present but with empty payload
    expect(textResponse).toEqual(
      "cloned NotVisibleInTs: undecodable exercise result test",
    );
    expect(newIcid).not.toEqual(nvitIcid);
    expect(archiveAndCreate).toMatchObject(expectedEvents);

    if (!("created" in archiveAndCreate[1]))
      throw "test above doesn't match below";
    const [
      ,
      {
        created: { payload: clonedPayload, key: clonedKey },
      },
    ] = archiveAndCreate;
    expect(clonedPayload).toEqual({});
    expect(clonedKey).toBeUndefined();
  });
});

test("createAndExercise", async () => {
  const ledger = new Ledger({ token: ALICE_TOKEN, httpBaseUrl: httpBaseUrl() });

  const [result, events] = await ledger.createAndExercise(
    buildAndLint.Main.Person.Birthday,
    { name: "Alice", party: ALICE_PARTY, age: "10", friends: [] },
    {},
  );
  expect(events).toMatchObject([
    {
      created: {
        templateId: buildAndLint.Main.Person.templateId,
        signatories: [ALICE_PARTY],
        payload: { name: "Alice", age: "10" },
      },
    },
    { archived: { templateId: buildAndLint.Main.Person.templateId } },
    {
      created: {
        templateId: buildAndLint.Main.Person.templateId,
        signatories: [ALICE_PARTY],
        payload: { name: "Alice", age: "11" },
      },
    },
  ]);
  expect(
    (events[0] as { created: { contractId: string } }).created.contractId,
  ).toEqual(
    (events[1] as { archived: { contractId: string } }).archived.contractId,
  );
  expect(result).toEqual(
    (events[2] as { created: { contractId: string } }).created.contractId,
  );
});

test("multi-{key,query} stream", async () => {
  const ledger = new Ledger({ token: ALICE_TOKEN, httpBaseUrl: httpBaseUrl() });

  function collect<T extends object, K, I extends string, State>(
    stream: Stream<T, K, I, State>,
  ): Promise<[State, readonly Event<T, K, I>[]][]> {
    const res = [] as [State, readonly Event<T, K, I>[]][];
    stream.on("change", (state, events) => res.push([state, events]));
    // wait until we’re live so that we get ordered transaction events
    // rather than unordered acs events.
    return new Promise(resolve => stream.on("live", () => resolve(res)));
  }
  async function create(t: string): Promise<void> {
    await ledger.create(buildAndLint.Main.Counter, {
      p: ALICE_PARTY,
      t,
      c: "0",
    });
  }
  async function update(t: string, c: number): Promise<void> {
    await ledger.exerciseByKey(
      buildAndLint.Main.Counter.Change,
      { _1: ALICE_PARTY, _2: t },
      { n: c.toString() },
    );
  }
  async function archive(t: string): Promise<void> {
    await ledger.archiveByKey(buildAndLint.Main.Counter, {
      _1: ALICE_PARTY,
      _2: t,
    });
  }
  async function close<T extends object, K, I extends string, State>(
    s: Stream<T, K, I, State>,
  ): Promise<void> {
    const p = pEvent(s, "close");
    s.close();
    await p;
  }
  // Add support for comparison queries
  /* eslint-disable @typescript-eslint/no-explicit-any */
  const streamQueriesWithComparison = ledger.streamQueries.bind(ledger) as (
    t: any,
    qs: any,
  ) => any;
  /* eslint-enable @typescript-eslint/no-explicit-any */
  const q = streamQueriesWithComparison(buildAndLint.Main.Counter, [
    { p: ALICE_PARTY, t: "included" },
    { c: { "%gt": 5 } },
  ]);
  const queryResult = await collect(q);
  const ks = ledger.streamFetchByKeys(buildAndLint.Main.Counter, [
    { _1: ALICE_PARTY, _2: "included" },
    { _1: ALICE_PARTY, _2: "byKey" },
    { _1: ALICE_PARTY, _2: "included" },
  ]);
  const byKeysResult = await collect(ks);

  await create("included");
  await create("byKey");
  await create("excluded");

  await update("excluded", 10);
  await update("byKey", 3);
  await update("byKey", 6);
  await update("excluded", 3);
  await update("included", 2);

  await archive("included");
  await archive("byKey");

  await create("included");

  await sleep(500);

  await close(q);
  await close(ks);

  expect(queryResult).toMatchObject([
    [
      [{ payload: { c: "0", t: "included" } }],
      [{ created: { payload: { c: "0", t: "included" } } }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "10", t: "excluded" } },
      ],
      [{ archived: {} }, { created: { payload: { c: "10", t: "excluded" } } }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "10", t: "excluded" } },
      ],
      [{ archived: {} }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "10", t: "excluded" } },
        { payload: { c: "6", t: "byKey" } },
      ],
      [{ archived: {} }, { created: { payload: { c: "6", t: "byKey" } } }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "6", t: "byKey" } },
      ],
      [{ archived: {} }],
    ],

    [
      [
        { payload: { c: "6", t: "byKey" } },
        { payload: { c: "2", t: "included" } },
      ],
      [{ archived: {} }, { created: { payload: { c: "2", t: "included" } } }],
    ],

    [[{ payload: { c: "6", t: "byKey" } }], [{ archived: {} }]],

    [[], [{ archived: {} }]],

    [
      [{ payload: { c: "0", t: "included" } }],
      [{ created: { payload: { c: "0", t: "included" } } }],
    ],
  ]);

  expect(byKeysResult).toMatchObject([
    [
      [
        { payload: { c: "0", t: "included" } },
        null,
        { payload: { c: "0", t: "included" } },
      ],
      [{ created: { payload: { c: "0", t: "included" } } }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "0", t: "byKey" } },
        { payload: { c: "0", t: "included" } },
      ],
      [{ created: { payload: { c: "0", t: "byKey" } } }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "3", t: "byKey" } },
        { payload: { c: "0", t: "included" } },
      ],
      [{ archived: {} }, { created: { payload: { c: "3", t: "byKey" } } }],
    ],

    [
      [
        { payload: { c: "0", t: "included" } },
        { payload: { c: "6", t: "byKey" } },
        { payload: { c: "0", t: "included" } },
      ],
      [{ archived: {} }, { created: { payload: { c: "6", t: "byKey" } } }],
    ],

    [
      [
        { payload: { c: "2", t: "included" } },
        { payload: { c: "6", t: "byKey" } },
        { payload: { c: "2", t: "included" } },
      ],
      [{ archived: {} }, { created: { payload: { c: "2", t: "included" } } }],
    ],

    [[null, { payload: { c: "6", t: "byKey" } }, null], [{ archived: {} }]],

    [[null, null, null], [{ archived: {} }]],

    [
      [
        { payload: { c: "0", t: "included" } },
        null,
        { payload: { c: "0", t: "included" } },
      ],
      [{ created: { payload: { c: "0", t: "included" } } }],
    ],
  ]);
});

test("stream close behaviour", async () => {
  const url = "ws" + httpBaseUrl().slice(4) + "v1/stream/query";
  const events: string[] = [];
  const ws = new WebSocket(url, ["jwt.token." + ALICE_TOKEN, "daml.ws.auth"]);
  await new Promise(resolve => ws.addEventListener("open", () => resolve()));
  const forCloseEvent = new Promise(resolve =>
    ws.addEventListener("close", () => {
      events.push("close");
      resolve();
    }),
  );
  events.push("before close");
  ws.close();
  events.push("after close");
  await forCloseEvent;

  expect(events).toEqual(["before close", "after close", "close"]);
});

test("party API", async () => {
  const p = (name: string, id: string): PartyInfo => ({
    identifier: id,
    displayName: name,
    isLocal: true,
  });
  const ledger = new Ledger({ token: ALICE_TOKEN, httpBaseUrl: httpBaseUrl() });
  const parties = await ledger.getParties([ALICE_PARTY, "unknown"]);
  expect(parties).toEqual([p("Alice", ALICE_PARTY), null]);
  const rev = await ledger.getParties(["unknown", ALICE_PARTY]);
  expect(rev).toEqual([null, p("Alice", ALICE_PARTY)]);

  const allParties = await ledger.listKnownParties();
  expect(_.sortBy(allParties, [(p: PartyInfo) => p.identifier])).toEqual([
    p("Alice", ALICE_PARTY),
    p("Bob", BOB_PARTY),
    p("Charlie", CHARLIE_PARTY),
    PARTICIPANT_PARTY_DETAILS,
  ]);

  const newParty1 = await ledger.allocateParty({});
  const newParty2 = await ledger.allocateParty({ displayName: "Carol" });
  const daveParty = (
    await ledger.allocateParty({ displayName: "Dave", identifierHint: "Dave" })
  ).identifier;

  const allPartiesAfter = (await ledger.listKnownParties()).map(
    pi => pi.identifier,
  );

  expect(_.sortBy(allPartiesAfter)).toEqual(
    _.sortBy([
      ALICE_PARTY,
      BOB_PARTY,
      CHARLIE_PARTY,
      PARTICIPANT_PARTY_DETAILS?.identifier,
      daveParty,
      newParty1.identifier,
      newParty2.identifier,
    ]),
  );
});

test("user API", async () => {
  const ledger = new Ledger({
    token: computeUserToken("participant_admin"),
    httpBaseUrl: httpBaseUrl(),
  });

  const participantAdminUser = await ledger.getUser();
  expect(participantAdminUser.userId).toEqual("participant_admin");
  expect(await ledger.listUserRights()).toEqual([
    UserRightHelper.participantAdmin,
  ]);

  const niceUser = "nice.user";
  const niceUserRights = [UserRightHelper.canActAs(ALICE_PARTY)];
  await ledger.createUser(niceUser, niceUserRights, ALICE_PARTY);

  expect(await ledger.getUser(niceUser)).toEqual({
    userId: niceUser,
    primaryParty: ALICE_PARTY,
  });
  expect(await ledger.listUserRights(niceUser)).toEqual(niceUserRights);
  expect(
    await ledger.grantUserRights(niceUser, [UserRightHelper.participantAdmin]),
  ).toEqual([UserRightHelper.participantAdmin]);
  expect(
    await ledger.revokeUserRights(niceUser, [
      UserRightHelper.participantAdmin,
      UserRightHelper.canActAs(ALICE_PARTY),
    ]),
  ).toEqual([
    UserRightHelper.participantAdmin,
    UserRightHelper.canActAs(ALICE_PARTY),
  ]);

  const allUserIds = (await ledger.listUsers()).map(it => it.userId);
  expect(_.sortBy(allUserIds)).toEqual([niceUser, "participant_admin"]);
  await ledger.deleteUser(niceUser);
  expect((await ledger.listUsers()).map(it => it.userId)).toEqual([
    "participant_admin",
  ]);
});

test("package API", async () => {
  // expect().toThrow does not seem to work with async thunk
  const expectFail = async <T>(p: Promise<T>): Promise<void> => {
    try {
      await p;
      expect(true).toBe(false);
    } catch (exc) {
      expect([400, 404]).toContain(exc.status);
      expect(exc.errors.length).toBe(1);
    }
  };
  const ledger = new Ledger({ token: ALICE_TOKEN, httpBaseUrl: httpBaseUrl() });

  const packagesBefore = await ledger.listPackages();

  expect(packagesBefore).toEqual(
    expect.arrayContaining([buildAndLint.packageId]),
  );
  expect(packagesBefore.length > 1).toBe(true);

  const nonSense = Uint8Array.from([1, 2, 3, 4]);

  await expectFail(ledger.uploadDarFile(nonSense));

  const upDar = await fs.readFile(getEnv("UPLOAD_DAR"));
  // throws on error
  await ledger.uploadDarFile(upDar);

  const packagesAfter = await ledger.listPackages();

  expect(packagesAfter).toEqual(
    expect.arrayContaining([buildAndLint.packageId]),
  );
  expect(packagesAfter.length > packagesBefore.length).toBe(true);
  expect(packagesAfter).toEqual(expect.arrayContaining(packagesBefore));

  await expectFail(ledger.getPackage("non-sense"));

  const downSuc = await ledger.getPackage(buildAndLint.packageId);
  expect(downSuc.byteLength > 0).toBe(true);
});

test("reconnect on timeout, when multiplexing is enabled", async () => {
  const charlieLedger = new Ledger({
    token: CHARLIE_TOKEN,
    httpBaseUrl: httpBaseUrl(),
    multiplexQueryStreams: true,
  });
  const charlieRawStream = charlieLedger.streamQuery(buildAndLint.Main.Person, {
    party: CHARLIE_PARTY,
  });
  const charlieStream = promisifyStream(charlieRawStream);
  const charlieStreamLive = pEvent(charlieRawStream, "live");
  expect(await charlieStreamLive).toEqual([]);

  const charlieRecord1: buildAndLint.Main.Person = {
    name: "Charlie Chaplin",
    party: CHARLIE_PARTY,
    age: "10",
    friends: [],
  };

  const charlieContract1 = await charlieLedger.create(
    buildAndLint.Main.Person,
    charlieRecord1,
  );
  expect(await charlieStream.next()).toEqual([
    [charlieContract1],
    [{ created: charlieContract1, matchedQueries: [0] }],
  ]);

  // wait 70s to trigger a disconnect on json-api which is configured to close conn after 1 minute.
  await new Promise(resolve => setTimeout(resolve, 70000));

  const charlieRecord2: buildAndLint.Main.Person = {
    name: "Charlie and the chocolate factory",
    party: CHARLIE_PARTY,
    age: "5",
    friends: [],
  };

  // ensure that we can write and read data post reconnect.
  const charlieContract2 = await charlieLedger.create(
    buildAndLint.Main.Person,
    charlieRecord2,
  );
  expect(await charlieStream.next()).toEqual([
    [charlieContract1, charlieContract2],
    [{ created: charlieContract2, matchedQueries: [0] }],
  ]);

  charlieStream.close();
});
