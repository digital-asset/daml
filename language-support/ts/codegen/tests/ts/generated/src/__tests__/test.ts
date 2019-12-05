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

const sandboxPort = 6865;
let sandboxProcess: ChildProcess;
const jsonApiPort = 7575;
let jsonApiProcess: ChildProcess;

beforeAll(async () => {
  const env = process.env;
  const envJavaPath = env['JAVA'];
  if (!envJavaPath) {
    throw Error('JAVA not set in environment.');
  }
  const javaPath: string = envJavaPath;
  const envDarPath = env['DAR'];
  if (!envDarPath) {
    throw Error('DAR not set in environment.');
  }
  const darPath: string = envDarPath;
  const envSandboxPath = env['SANDBOX'];
  if (!envSandboxPath) {
    throw Error('SANDBOX not set in environment.');
  }
  const sandboxPath: string = envSandboxPath;
  sandboxProcess = spawn(javaPath, ['-jar', sandboxPath, '--port', `${sandboxPort}`, '--ledgerid', LEDGER_ID, '--wall-clock-time', darPath], {
    env: env,
    stdio: "inherit",
  });
  await waitOn({resources: [`tcp:localhost:${sandboxPort}`]})
  console.log('Sandbox up');
  const envJsonApiPath = env['JSON_API'];
  if (!envJsonApiPath) {
    throw Error('JSON_API not set in environment.');
  }
  const jsonApiPath: string = envJsonApiPath;
  jsonApiProcess = spawn(javaPath, ['-jar', jsonApiPath, '--ledger-host', 'localhost', '--ledger-port', `${sandboxPort}`,'--http-port', `${jsonApiPort}`], {
    env: env,
    stdio: "inherit",
  });
  await waitOn({resources: [`tcp:localhost:${jsonApiPort}`]})
  console.log('JSON API up');
});

afterAll(() => {
  sandboxProcess.kill("SIGTERM");
  console.log('Killed sandbox');
  jsonApiProcess.kill("SIGTERM");
  console.log('Killed JSON API');
});

test('hello', async () => {
  const ledger = new Ledger(ALICE_TOKEN, `http://localhost:${jsonApiPort}/`);
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
});
