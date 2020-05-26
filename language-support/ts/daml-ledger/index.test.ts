// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Template, Choice, ContractId } from "@daml/types";
import Ledger, {CreateEvent} from "./index";
import { Event } from "./index";
import * as jtv from "@mojotech/json-type-validation";
import type { EventEmitter } from 'events';
import mockConsole from "jest-mock-console";

const mockLive = jest.fn();
const mockChange = jest.fn();
const mockConstructor = jest.fn();
const mockSend = jest.fn();
const mockClose = jest.fn();
const mockFunctions = [mockLive, mockChange, mockConstructor, mockSend, mockClose];

type Foo = {key: string};
const fooKey = 'fooKey';

type Message =
  | { events: Event<Foo>[]; offset?: string }
  | { warnings: string[] }
  | { errors: string[] }
  | string //for unexpected messages

interface MockWebSocket {
  serverOpen(): void;
  serverSend(message: Message): void;
  serverClose(event: {code: number; reason: string}): void;
}

let mockInstance = undefined as unknown as MockWebSocket;

jest.mock('isomorphic-ws', () => class {
  private eventEmitter: EventEmitter;

  constructor(...args: unknown[]) {
    mockConstructor(...args);
    mockInstance = this;
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const {EventEmitter} = require('events');
    this.eventEmitter = new EventEmitter();
  }

  addEventListener(event: string, handler: (...args: unknown[]) => void): void {
    this.eventEmitter.on(event, handler);
  }

  send(message: string): void {
    mockSend(JSON.parse(message));
  }

  serverOpen(): void {
    this.eventEmitter.emit('open');
  }

  serverSend(message: Message): void {
    this.eventEmitter.emit('message', {data: JSON.stringify(message)});
  }

  serverClose(event: {code: number; reason: string}): void {
    this.eventEmitter.emit('close', event);
  }
});



const Foo: Template<Foo, string, "foo-id"> = {
  templateId: "foo-id",
  keyDecoder: () => jtv.string(),
  decoder: () => jtv.object({key: jtv.string()}),
  Archive: {} as unknown as Choice<Foo, {}, {}, string>,
};

const fooCreateEvent = (
  coid: number
): CreateEvent<Foo, string, "foo-id"> => {
  return {
    templateId: "foo-id",
    contractId: coid.toString() as ContractId<Foo>,
    signatories: [],
    observers: [],
    agreementText: "fooAgreement",
    key: fooKey,
    payload: {key: fooKey},
  };
};

const fooEvent = (coid: number): Event<Foo, string, "foo-id"> => {
  return { created: fooCreateEvent(coid) };
};


const fooArchiveEvent = (coid: number): Event<Foo, string, "foo-id"> => {
  return {
    archived: {
      templateId: "foo-id",
      contractId: coid.toString() as ContractId<Foo>,
    },
  };
};

const mockOptions = {
  token: "dummyToken",
  httpBaseUrl: "http://localhost:5000/",
  wsBaseUrl: "ws://localhost:4000/",
};

beforeEach(() => {
  mockFunctions.forEach(f => f.mockClear());
});

describe("streamQuery", () => {
  test("receive unknown message", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    expect(mockConstructor).toHaveBeenCalledTimes(1);
    expect(mockConstructor).toHaveBeenLastCalledWith(
      'ws://localhost:4000/v1/stream/query',
      ['jwt.token.dummyToken', 'daml.ws.auth'],
    );
    stream.on("change", mockChange);

    mockInstance.serverOpen();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(mockSend).toHaveBeenLastCalledWith({templateIds: [Foo.templateId]});
    const restoreConsole = mockConsole();
    mockInstance.serverSend('mickey mouse');
    expect(console.error).toHaveBeenCalledWith("Ledger.streamQuery unknown message", "mickey mouse");
    restoreConsole();
  });

  test("receive warnings", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", mockChange);
    const restoreConsole = mockConsole();
    mockInstance.serverSend({ warnings: ["oh oh"] });
    expect(console.warn).toHaveBeenCalledWith("Ledger.streamQuery warnings", {"warnings": ["oh oh"]});
    restoreConsole();
  });

  test("receive errors", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", mockChange);
    const restoreConsole = mockConsole();
    mockInstance.serverSend({ errors: ["not good!"] });
    expect(console.error).toHaveBeenCalledWith("Ledger.streamQuery errors", { errors: ["not good!"] });
    restoreConsole();
  });

  test("receive live event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("live", mockLive);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1)], offset: '3' });
    expect(mockLive).toHaveBeenCalledTimes(1);
    expect(mockLive).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([fooCreateEvent(1)])
  });

  test("reconnect on close", async () => {
    const reconnectThreshold = 200;
    const ledger = new Ledger({...mockOptions, reconnectThreshold: reconnectThreshold});
    const stream = ledger.streamQuery(Foo);
    stream.on("live", mockLive);
    stream.on("close", mockClose);
    mockInstance.serverSend({events: [], offset: '3'});
    await new Promise(resolve => setTimeout(resolve, reconnectThreshold));
    mockConstructor.mockClear();
    mockInstance.serverClose({code: 1, reason: 'test close'});
    expect(mockConstructor).toHaveBeenCalled();
    mockInstance.serverOpen();
    expect(mockSend).toHaveBeenNthCalledWith(1, {offset: "3"});
    expect(mockSend).toHaveBeenNthCalledWith(2, [{"templateIds": ["foo-id"]}]);
    mockSend.mockClear();
    mockConstructor.mockClear();

    // check that the client doesn't try to reconnect again.  it should only reconnect if it
    // received an event confirming the stream is live again, i.e. {events: [], offset: '3'}
    mockInstance.serverClose({code: 1, reason: 'test close'});
    expect(mockConstructor).not.toHaveBeenCalled();
  });

  test("receive empty events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [] });
    expect(mockChange).toHaveBeenCalledTimes(0);
  });

  test("receive one event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
  });

  test("receive several events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [1, 2, 3].map(fooEvent) });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([1, 2, 3].map(fooCreateEvent));
  });

  test("drop matching created and archived events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1), fooEvent(2)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(1), fooCreateEvent(2)]);
    mockChange.mockClear();
    mockInstance.serverSend({ events: [fooArchiveEvent(1)]});
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(2)]);
  });
});

describe("streamFetchByKey", () => {
  test("receive no event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKey(Foo, 'badKey');
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [] });
    expect(mockChange).toHaveBeenCalledTimes(0);
  });

  test("receive one event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKey(Foo, fooKey);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(fooCreateEvent(1));
  });

  test("receive several events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKey(Foo, fooKey);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1), fooEvent(2), fooEvent(3)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(fooCreateEvent(3));
  });

  test("drop matching created and archived events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKey(Foo, fooKey);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(fooCreateEvent(1));
    mockChange.mockClear();
    mockInstance.serverSend({ events: [fooArchiveEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(null);
  });

  test("reconnect on close", async () => {
    const reconnectThreshold = 200;
    const ledger = new Ledger({...mockOptions, reconnectThreshold: reconnectThreshold} );
    const stream = ledger.streamFetchByKey(Foo, fooKey);
    stream.on("live", mockLive);
    stream.on("close", mockClose);
    //send live event, but no contract yet.
    mockInstance.serverSend({events: [], offset: '3'});
    await new Promise(resolve => setTimeout(resolve, reconnectThreshold));
    mockConstructor.mockClear();
    mockInstance.serverClose({code: 1, reason: 'test close'});
    expect(mockConstructor).toHaveBeenCalled();
    mockInstance.serverOpen();
    expect(mockSend).toHaveBeenCalledTimes(2)
    expect(mockSend).toHaveBeenNthCalledWith(1, {'offset': '3'});
    expect(mockSend).toHaveBeenNthCalledWith(2, [{'key': 'fooKey', 'templateId': 'foo-id', 'contractIdAtOffset': null}]);

    //send live event and set the last contract id.
    mockInstance.serverSend({events: [fooEvent(1)], offset: '4'});
    await new Promise(resolve => setTimeout(resolve, reconnectThreshold));
    mockConstructor.mockClear();
    mockInstance.serverClose({code: 1, reason: 'second test close'});
    expect(mockConstructor).toHaveBeenCalled();
    mockSend.mockClear();
    mockInstance.serverOpen();
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend).toHaveBeenNthCalledWith(1, {'offset': '4'});
    expect(mockSend).toHaveBeenNthCalledWith(2, [{'key': 'fooKey', 'templateId': 'foo-id', 'contractIdAtOffset': '1'}]);
    mockSend.mockClear();
    mockConstructor.mockClear();

    // check that the client doesn't try to reconnect again.  it should only reconnect if it
    // received an event confirming the stream is live again, i.e. {events: [], offset: '3'}
    mockInstance.serverClose({code: 1, reason: 'test close'});
    expect(mockConstructor).not.toHaveBeenCalled();
  });
});
