// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// The tests here are identical to the ones in
// templates/create-daml-app-test-resources/index.test.ts.
// The main difference is in the test setup. Since we want to be able to run
// this against a different platform version than SDK version we use `daml sandbox`
// and `daml json-api` directly instead of going via `daml start`.
// In addition to that, these tests here work on Windows.
// We could in principle just drop the other tests. However, they are
// used in documentation which we probably don’t want to make
// even more complex and you can argue that there is value in testing
// `daml start` which is what users use.

import child_process, { ChildProcess, execFileSync, spawn, SpawnOptions } from "child_process";
import { promises as fs } from "fs";
import { encode } from 'jwt-simple';
import puppeteer, { Browser, Page } from "puppeteer";
import waitOn from "wait-on";

import Ledger, { UserRightHelper, UserRight } from "@daml/ledger";
import { User } from "@daml.js/create-daml-app";
import semver from "semver";

/* API has changed; depending on version loaded, only one of these two imports
 * will work, but invalid imports result in undefined variables, not failure,
 * so we can work around that.
 */
import { authConfig } from './config';
import { insecure } from "./config";

const getToken: (party: string) => string =
    insecure
        ? (party) => insecure.makeToken(party)
        : (party) => authConfig.makeToken(party);

const DAR_PATH = process.env.DAR_PATH;
const SANDBOX_PORT_FILE_NAME = "sandbox.port";
const JSON_API_PORT_FILE_NAME = "json-api.port";
const SANDBOX_PORT_FILE_PATH = `../${SANDBOX_PORT_FILE_NAME}`;
const JSON_API_PORT_FILE_PATH = `../${JSON_API_PORT_FILE_NAME}`;

const JSON_API_PORT = 7575;
const UI_PORT = 3000;

let sandbox: ChildProcess | undefined = undefined;

// `npm start` process
let uiProc: ChildProcess | undefined = undefined;

// Chrome browser that we run in headless mode
let browser: Browser | undefined = undefined;

let publicUser: string | undefined = undefined;
let publicParty: string | undefined = undefined;

const adminLedger = new Ledger({
  token: getToken("participant_admin"),
  httpBaseUrl: "http://127.0.0.1:7575/",
});

const toAlias = (userId: string): string =>
  userId.charAt(0).toUpperCase() + userId.slice(1);

// Function to generate unique party names for us.
let nextPartyId = 1;
const getParty = async(): Promise<[string, string]> => {
  const allocResult = await adminLedger.allocateParty({});
  const user = `u${nextPartyId}`;
  const party = allocResult.identifier;
  const rights: UserRight[] = [UserRightHelper.canActAs(party)].concat(
      publicParty !== undefined ? [UserRightHelper.canReadAs(publicParty)] : [],
  );
  await adminLedger.createUser(user, rights, party);
  nextPartyId++;
  return [user, party];
};

test("Party names are unique", async () => {
  let r: string[] = [];
  for (let i = 0; i < 10; ++i) {
    r = r.concat((await getParty())[1]);
  }
  const parties = new Set(r);
  expect(parties.size).toEqual(10);
}, 20_000);

const removeFile = async (path: string) => {
  try {
    await fs.stat(path);
    await fs.unlink(path);
  } catch (_e) {
    // Do nothing if the file does not exist.
  }
};

// promisified-version of exec.
const exec = (command) => {
  return new Promise((done, failed) => {
    child_process.exec(command, {}, (err, stdout, stderr) => {
      if (err) {
        err.stdout = stdout;
        err.stderr = stderr;
        failed(err);
        return;
      }

      done({ stdout, stderr });
    });
  });
};

// The node.js process API doesn’t seem to even attempt
// to provide sensible semantics across platforms:
// On Unix you can set detached: true which will create a new
// process group and then kill the whole group by passing a negative pid.
// On Windows, detached does not create a new group. Instead it runs
// the process in a new (graphical by default) terminal.
// There is no way to use `process.kill` to kill a process including
// its children. You have to shell out to taskkill instead.
const killTree = async (p: ChildProcess) => {
  if (process.platform == "win32") {
    await exec(`taskkill /pid ${p.pid} /t /f`);
  } else {
    process.kill(-p.pid);
  }
};

const detached = process.platform != "win32";

const npmExeName = process.platform == "win32" ? "npm" : "npm-cli.js";

// Start the Daml and UI processes before the tests begin.
// To reduce test times, we reuse the same processes between all the tests.
// This means we need to use a different set of parties and a new browser page for each test.
beforeAll(async () => {
  await removeFile(`../${SANDBOX_PORT_FILE_NAME}`);
  await removeFile(`../${JSON_API_PORT_FILE_NAME}`);

  const sandboxOptions = [
    "sandbox",
    "--canton-port-file",
    `${SANDBOX_PORT_FILE_NAME}`,
    `--json-api-port=${JSON_API_PORT}`,
    `--json-api-port-file=${JSON_API_PORT_FILE_NAME}`,
  ];

  sandbox = spawn(process.env.DAML_SANDBOX, sandboxOptions, {
    cwd: "..",
    stdio: "inherit",
    detached: detached,
    env: { ...process.env, DAML_SDK_VERSION: process.env.SANDBOX_VERSION },
  });

  interface CantonPorts {
    sandbox: {
      ledgerApi: number
      adminApi: number
    }
  }

  await waitOn({ resources: [`file:../${SANDBOX_PORT_FILE_NAME}`] });
  const contents = await fs.readFile(SANDBOX_PORT_FILE_PATH, "utf8")
  const cantonPorts: CantonPorts = JSON.parse(contents);

  const sandboxPort = cantonPorts.sandbox.ledgerApi
  execFileSync(process.env.DAML, ["ledger", "upload-dar", "--host=127.0.0.1", `--port=${sandboxPort}`, DAR_PATH])

  await waitOn({ resources: [`file:../${JSON_API_PORT_FILE_NAME}`] });

  [publicUser, publicParty] = await getParty();

  const env = { ...process.env, BROWSER: "none" };
  uiProc =
    spawn(npmExeName, ["start"], {
    env,
    stdio: "inherit",
    shell: true,
    detached: detached,
  });

  await waitOn({ resources: [`tcp:127.0.0.1:${UI_PORT}`] });

  // Launch a single browser for all tests.
  browser = await puppeteer.launch();
}, 60_000);

afterAll(async () => {
  if (browser) {
    await browser.close();
  }
  if (uiProc) {
    await killTree(uiProc);
  }
  if (sandbox) {
    await killTree(sandbox);
  }
}, 60_000);

test("create and look up user using ledger library", async () => {
  const [user, party] = await getParty();
  const token = getToken(user);
  const ledger = new Ledger({ token });
  const users0 = await ledger.query(User.User);
  expect(users0).toEqual([]);
  const userPayload = { username: party, following: [], public: publicParty };
  const userContract1 = await ledger.create(User.User, userPayload);
  const userContract2 = await ledger.fetchByKey(User.User, party);
  expect(userContract1).toEqual(userContract2);
  const users = await ledger.query(User.User);
  expect(users[0]).toEqual(userContract1);
}, 60_000);

// The tests following use the headless browser to interact with the app.
// We select the relevant DOM elements using CSS class names that we embedded
// specifically for testing.
// See https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors.

const newUiPage = async (): Promise<Page> => {
  if (!browser) {
    throw Error("Puppeteer browser has not been launched");
  }
  const page = await browser.newPage();
  await page.setDefaultNavigationTimeout(60 * 1000);
  await page.goto(`http://127.0.0.1:${UI_PORT}`); // ignore the Response
  return page;
};

const waitForNSelector = async (page: Page, selector: string, n: number) => {
  await page.waitForFunction(
      (n, selector) => document.querySelectorAll(selector).length == n,
      {},
      n,
      selector
  );
}

// Note that Follow is a consuming choice on a contract
// with a contract key so it is crucial to wait between follows.
// Otherwise, you get errors due to contention.
// Those can manifest in puppeteer throwing `Target closed`
// but that is not the underlying error (the JSON API will
// output the contention errors as well so look through the log).
const waitForFollowers = async (page: Page, n: number) => {
  await waitForNSelector(page, ".test-select-following", n);
}

// LOGIN_FUNCTION_BEGIN
// Log in using a party name and wait for the main screen to load.
const login = async (page: Page, partyName: string) => {
  const usernameInput = await page.waitForSelector(
    ".test-select-username-field",
  );
  if (usernameInput) {
    await usernameInput.click();
    await usernameInput.type(partyName);
    await page.click(".test-select-login-button");
    await page.waitForSelector(".test-select-main-menu");
  }
};
// LOGIN_FUNCTION_END

// Log out and wait to get back to the login screen.
const logout = async (page: Page) => {
  await page.click(".test-select-log-out");
  await page.waitForSelector(".test-select-login-screen");
};

// Follow a user using the text input in the follow panel.
const follow = async (page: Page, userToFollow: string) => {
  const followInput = await page.waitForSelector(".test-select-follow-input");
  if (followInput) {
    await followInput.click();
    await followInput.type(userToFollow);
    await followInput.press('Tab');
    await page.click('.test-select-follow-button');

    // Wait for the request to complete, either successfully or after the error
    // dialog has been handled.
    // We check this by the absence of the `loading` class.
    // (Both the `test-...` and `loading` classes appear in `div`s surrounding
    // the `input`, due to the translation of Semantic UI's `Input` element.)
    await page.waitForSelector(".test-select-follow-input > :not(.loading)", {
      timeout: 60_000,
    });
  }
};

// LOGIN_TEST_BEGIN
test("log in as a new user, log out and log back in", async () => {
  const [user, party] = await getParty();

  // Log in as a new user.
  const page = await newUiPage();
  await login(page, user);

  // Check that the ledger contains the new User contract.
  const token = getToken(user);
  const ledger = new Ledger({ token });
  const users = await ledger.query(User.User);
  expect(users).toHaveLength(1);
  expect(users[0].payload.username).toEqual(party);

  // Log out and in again as the same user.
  await logout(page);
  await login(page, user);

  // Check we have the same one user.
  const usersFinal = await ledger.query(User.User);
  expect(usersFinal).toHaveLength(1);
  expect(usersFinal[0].payload.username).toEqual(party);

  await page.close();
}, 60_000);
// LOGIN_TEST_END

// This tests following users in a few different ways:
// - using the text box in the Follow panel
// - using the icon in the Network panel
// - while the user that is followed is logged in
// - while the user that is followed is logged out
// These are all successful cases.
test("log in as three different users and start following each other", async () => {
  const [user1, party1] = await getParty();
  const [user2, party2] = await getParty();
  const [user3, party3] = await getParty();

  // Log in as Party 1.
  const page1 = await newUiPage();
  await login(page1, user1);

  // Log in as Party 2.
  const page2 = await newUiPage();
  await login(page2, user2);

  // Log in as Party 3.
  const page3 = await newUiPage();
  await login(page3, user3);

  // Party 1 should initially follow no one.
  const noFollowing1 = await page1.$$(".test-select-following");
  expect(noFollowing1).toEqual([]);

  // Follow Party 2 using the text input.
  // This should work even though Party 2 has not logged in yet.
  // Check Party 1 follows exactly Party 2.
  await follow(page1, party2);
  await waitForFollowers(page1, 1);
  const followingList1 = await page1.$$eval(
    ".test-select-following",
    (following) => following.map((e) => e.innerHTML)
  );
  expect(followingList1).toEqual([toAlias(user2)]);

  // Add Party 3 as well and check both are in the list.
  await follow(page1, party3);
  await waitForFollowers(page1, 2);
  const followingList11 = await page1.$$eval(
    ".test-select-following",
    (following) => following.map((e) => e.innerHTML)
  );
  expect(followingList11).toHaveLength(2);
  expect(followingList11).toContain(toAlias(user2));
  expect(followingList11).toContain(toAlias(user3));

  // Party 2 should initially follow no one.
  const noFollowing2 = await page2.$$(".test-select-following");
  expect(noFollowing2).toEqual([]);

  // However, Party 2 should see Party 1 in the network.
  await page2.waitForSelector(".test-select-user-in-network");
  const network2 = await page2.$$eval(".test-select-user-in-network", (users) =>
    users.map((e) => e.innerHTML)
  );
  expect(network2).toEqual([toAlias(user1)]);

  // Follow Party 1 using the 'add user' icon on the right.
  await page2.waitForSelector(".test-select-add-user-icon");
  const userIcons = await page2.$$(".test-select-add-user-icon");
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
  const followingList2 = await page2.$$eval(
    ".test-select-following",
    (following) => following.map((e) => e.innerHTML)
  );
  expect(followingList2).toHaveLength(2);
  expect(followingList2).toContain(toAlias(user1));
  expect(followingList2).toContain(toAlias(user3));

  // Party 1 should now also see Party 2 in the network (but not Party 3 as they
  // didn't yet started following Party 1).
  await page1.waitForSelector(".test-select-user-in-network");
  const network1 = await page1.$$eval(
    ".test-select-user-in-network",
    (following) => following.map((e) => e.innerHTML)
  );
  expect(network1).toEqual([toAlias(user2)]);

  // Party 3 should follow no one.
  const noFollowing3 = await page3.$$(".test-select-following");
  expect(noFollowing3).toEqual([]);

  // However, Party 3 should see both Party 1 and Party 2 in the network.
  await waitForNSelector(page3, ".test-select-user-in-network", 2);

  const network3 = await page3.$$eval(
    ".test-select-user-in-network",
    (following) => following.map((e) => e.innerHTML)
  );
  expect(network3).toHaveLength(2);
  expect(network3).toContain(toAlias(user1));
  expect(network3).toContain(toAlias(user2));

  await page1.close();
  await page2.close();
  await page3.close();
}, 60_000);

test("error when following self", async () => {
  const [user, party] = await getParty();
  const page = await newUiPage();

  const dismissError = jest.fn((dialog) => dialog.dismiss());
  page.on("dialog", dismissError);

  await login(page, user);
  await follow(page, party);

  expect(dismissError).toHaveBeenCalled();

  await page.close();
}, 60_000);

test("error when adding a user that you are already following", async () => {
  const [user1, party1] = await getParty();
  const [user2, party2] = await getParty();
  const page = await newUiPage();

  const dismissError = jest.fn((dialog) => dialog.dismiss());
  page.on("dialog", dismissError);

  await login(page, user1);
  // First attempt should succeed
  await follow(page, party2);
  // Second attempt should result in an error
  await follow(page, party2);

  expect(dismissError).toHaveBeenCalled();

  await page.close();
}, 60_000);