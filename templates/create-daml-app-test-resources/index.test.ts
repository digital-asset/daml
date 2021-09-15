// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Keep in sync with compatibility/bazel_tools/create-daml-app/index.test.ts

import { ChildProcess, spawn, SpawnOptions } from 'child_process';
import { promises as fs } from 'fs';
import puppeteer, { Browser, Page } from 'puppeteer';
import waitOn from 'wait-on';

import Ledger from '@daml/ledger';
import { User } from '@daml.js/create-daml-app';
import { authConfig } from './config';

const JSON_API_PORT_FILE_NAME = 'json-api.port';

const UI_PORT = 3000;

// `daml start` process
let startProc: ChildProcess | undefined = undefined;

// `npm start` process
let uiProc: ChildProcess | undefined = undefined;

// Chrome browser that we run in headless mode
let browser: Browser | undefined = undefined;

// Function to generate unique party names for us.
// This should be replaced by the party management service once that is exposed
// in the HTTP JSON API.
let nextPartyId = 1;
function getParty(): string {
  const party = `P${nextPartyId}`;
  nextPartyId++;
  return party;
}

test('Party names are unique', async () => {
  const parties = new Set(Array(10).fill({}).map(() => getParty()));
  expect(parties.size).toEqual(10);
});

const removeFile = async (path: string) => {
  try {
    await fs.stat(path);
    await fs.unlink(path);
  } catch (_e) {
    // Do nothing if the file does not exist.
  }
}

// Start the Daml and UI processes before the tests begin.
// To reduce test times, we reuse the same processes between all the tests.
// This means we need to use a different set of parties and a new browser page for each test.
beforeAll(async () => {
  // If the JSON API server was previously shut down abruptly then the port file
  // may not have been removed.
  // Since we use this file to know when the server is up, we remove it first
  // (if it exists) to be sure.
  const jsonApiPortFilePath = `../${JSON_API_PORT_FILE_NAME}`; // relative to ui folder
  await removeFile(jsonApiPortFilePath);

  // Run `daml start` from the project root (where the `daml.yaml` is located).
  // The path should include '.daml/bin' in the environment where this is run,
  // which contains the `daml` assistant executable.
  const startOpts: SpawnOptions = { cwd: '..', stdio: 'inherit' };

  // Arguments for `daml start` (besides those in the `daml.yaml`).
  // The JSON API `--port-file` gives us a file we can check to know that both
  // the sandbox and JSON API server are up and running.
  // We use the default ports for the sandbox and JSON API as done in the
  // Getting Started Guide.
  const startArgs = [
    'start',
    `--json-api-option=--port-file=${JSON_API_PORT_FILE_NAME}`,
  ];

  startProc = spawn('daml', startArgs, startOpts);

  await waitOn({resources: [`file:${jsonApiPortFilePath}`]});

  // Run `npm start` in another shell.
  // Disable automatically opening a browser using the env var described here:
  // https://github.com/facebook/create-react-app/issues/873#issuecomment-266318338
  const env = {...process.env, BROWSER: 'none'};
  uiProc = spawn('npm-cli.js', ['run-script', 'start'], { env, stdio: 'inherit', detached: true});
  // Note(kill-npm-start): The `detached` flag starts the process in a new process group.
  // This allows us to kill the process with all its descendents after the tests finish,
  // following https://azimi.me/2014/12/31/kill-child_process-node-js.html.

  // Ensure the UI server is ready by checking that the port is available.
  await waitOn({resources: [`tcp:localhost:${UI_PORT}`]});

  // Launch a single browser for all tests.
  browser = await puppeteer.launch();
}, 40_000);

afterAll(async () => {
  // Kill the `daml start` process, allowing the sandbox and JSON API server to
  // shut down gracefully.
  // The latter process should also remove the JSON API port file.
  // TODO: Test this on Windows.
  if (startProc) {
    startProc.kill('SIGTERM');
  }

  // Kill the `npm start` process including all its descendents.
  // The `-` indicates to kill all processes in the process group.
  // See Note(kill-npm-start).
  // TODO: Test this on Windows.
  if (uiProc) {
    process.kill(-uiProc.pid)
  }

  if (browser) {
    browser.close();
  }
});

test('create and look up user using ledger library', async () => {
  const party = getParty();
  const token = authConfig.makeToken(party);
  const ledger = new Ledger({token});
  const users0 = await ledger.query(User.User);
  expect(users0).toEqual([]);
  const user = {username: party, following: []};
  const userContract1 = await ledger.create(User.User, user);
  const userContract2 = await ledger.fetchByKey(User.User, party);
  expect(userContract1).toEqual(userContract2);
  const users = await ledger.query(User.User);
  expect(users[0]).toEqual(userContract1);
});

// The tests following use the headless browser to interact with the app.
// We select the relevant DOM elements using CSS class names that we embedded
// specifically for testing.
// See https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors.

const newUiPage = async (): Promise<Page> => {
  if (!browser) {
    throw Error('Puppeteer browser has not been launched');
  }
  const page = await browser.newPage();
  await page.goto(`http://localhost:${UI_PORT}`); // ignore the Response
  return page;
}

// Note that Follow is a consuming choice on a contract
// with a contract key so it is crucial to wait between follows.
// Otherwise, you get errors due to contention.
// Those can manifest in puppeteer throwing `Target closed`
// but that is not the underlying error (the JSON API will
// output the contention errors as well so look through the log).
const waitForFollowers = async (page: Page, n: number) => {
  await page.waitForFunction(
      (n) => document.querySelectorAll(".test-select-following").length == n,
      {},
      n
  );
}

// LOGIN_FUNCTION_BEGIN
// Log in using a party name and wait for the main screen to load.
const login = async (page: Page, partyName: string) => {
  const usernameInput = await page.waitForSelector('.test-select-username-field');
  await usernameInput.click();
  await usernameInput.type(partyName);
  await page.click('.test-select-login-button');
  await page.waitForSelector('.test-select-main-menu');
}
// LOGIN_FUNCTION_END

// Log out and wait to get back to the login screen.
const logout = async (page: Page) => {
  await page.click('.test-select-log-out');
  await page.waitForSelector('.test-select-login-screen');
}

// Follow a user using the text input in the follow panel.
const follow = async (page: Page, userToFollow: string) => {
  await page.click('.test-select-follow-input');
  await page.type('.test-select-follow-input', userToFollow);
  await page.click('.test-select-follow-button');

  // Wait for the request to complete, either successfully or after the error
  // dialog has been handled.
  // We check this by the absence of the `loading` class.
  // (Both the `test-...` and `loading` classes appear in `div`s surrounding
  // the `input`, due to the translation of Semantic UI's `Input` element.)
  await page.waitForSelector('.test-select-follow-input > :not(.loading)', {timeout: 40_000});
}

// LOGIN_TEST_BEGIN
test('log in as a new user, log out and log back in', async () => {
  const partyName = getParty();

  // Log in as a new user.
  const page = await newUiPage();
  await login(page, partyName);

  // Check that the ledger contains the new User contract.
  const token = authConfig.makeToken(partyName);
  const ledger = new Ledger({token});
  const users = await ledger.query(User.User);
  expect(users).toHaveLength(1);
  expect(users[0].payload.username).toEqual(partyName);

  // Log out and in again as the same user.
  await logout(page);
  await login(page, partyName);

  // Check we have the same one user.
  const usersFinal = await ledger.query(User.User);
  expect(usersFinal).toHaveLength(1);
  expect(usersFinal[0].payload.username).toEqual(partyName);

  await page.close();
}, 40_000);
// LOGIN_TEST_END

// This tests following users in a few different ways:
// - using the text box in the Follow panel
// - using the icon in the Network panel
// - while the user that is followed is logged in
// - while the user that is followed is logged out
// These are all successful cases.

test('log in as three different users and start following each other', async () => {
  const party1 = getParty();
  const party2 = getParty();
  const party3 = getParty();

  // Log in as Party 1.
  const page1 = await newUiPage();
  await login(page1, party1);

  // Party 1 should initially follow no one.
  const noFollowing1 = await page1.$$('.test-select-following');
  expect(noFollowing1).toEqual([]);

  // Follow Party 2 using the text input.
  // This should work even though Party 2 has not logged in yet.
  // Check Party 1 follows exactly Party 2.
  await follow(page1, party2);
  await waitForFollowers(page1, 1);
  const followingList1 = await page1.$$eval('.test-select-following', following => following.map(e => e.innerHTML));
  expect(followingList1).toEqual([party2]);

   // Add Party 3 as well and check both are in the list.
   await follow(page1, party3);
   await waitForFollowers(page1, 2);
   const followingList11 = await page1.$$eval('.test-select-following', following => following.map(e => e.innerHTML));
   expect(followingList11).toHaveLength(2);
   expect(followingList11).toContain(party2);
   expect(followingList11).toContain(party3);

  // Log in as Party 2.
  const page2 = await newUiPage();
  await login(page2, party2);

  // Party 2 should initially follow no one.
  const noFollowing2 = await page2.$$('.test-select-following');
  expect(noFollowing2).toEqual([]);

  // However, Party 2 should see Party 1 in the network.
  await page2.waitForSelector('.test-select-user-in-network');
  const network2 = await page2.$$eval('.test-select-user-in-network', users => users.map(e => e.innerHTML));
  expect(network2).toEqual([party1]);

  // Follow Party 1 using the 'add user' icon on the right.
  await page2.waitForSelector('.test-select-add-user-icon');
  const userIcons = await page2.$$('.test-select-add-user-icon');
  expect(userIcons).toHaveLength(1);
  await userIcons[0].click();
  await waitForFollowers(page2, 1);

  // Also follow Party 3 using the text input.
  // Note that we can also use the icon to follow Party 3 as they appear in the
  // Party 1's Network panel, but that's harder to test at the
  // moment because there is no loading indicator to tell when it's done.
  await follow(page2, party3);

  // Check the following list is updated correctly.
  await waitForFollowers(page2, 2);
  const followingList2 = await page2.$$eval('.test-select-following', following => following.map(e => e.innerHTML));
  expect(followingList2).toHaveLength(2);
  expect(followingList2).toContain(party1);
  expect(followingList2).toContain(party3);

  // Party 1 should now also see Party 2 in the network (but not Party 3 as they
  // didn't yet started following Party 1).
  await page1.waitForSelector('.test-select-user-in-network');
  const network1 = await page1.$$eval('.test-select-user-in-network', following => following.map(e => e.innerHTML));
  expect(network1).toEqual([party2]);

  // Log in as Party 3.
  const page3 = await newUiPage();
  await login(page3, party3);

  // Party 3 should follow no one.
  const noFollowing3 = await page3.$$('.test-select-following');
  expect(noFollowing3).toEqual([]);

  // However, Party 3 should see both Party 1 and Party 2 in the network.
  await page3.waitForSelector('.test-select-user-in-network');
  const network3 = await page3.$$eval('.test-select-user-in-network', following => following.map(e => e.innerHTML));
  expect(network3).toHaveLength(2);
  expect(network3).toContain(party1);
  expect(network3).toContain(party2);

  await page1.close();
  await page2.close();
  await page3.close();
}, 60_000);

test('error when following self', async () => {
  const party = getParty();
  const page = await newUiPage();

  const dismissError = jest.fn(dialog => dialog.dismiss());
  page.on('dialog', dismissError);

  await login(page, party);
  await follow(page, party);

  expect(dismissError).toHaveBeenCalled();

  await page.close();
});

test('error when adding a user that you are already following', async () => {
  const party1 = getParty();
  const party2 = getParty();
  const page = await newUiPage();

  const dismissError = jest.fn(dialog => dialog.dismiss());
  page.on('dialog', dismissError);

  await login(page, party1);
  // First attempt should succeed
  await follow(page, party2);
  // Second attempt should result in an error
  await follow(page, party2);

  expect(dismissError).toHaveBeenCalled();

  await page.close();
});
