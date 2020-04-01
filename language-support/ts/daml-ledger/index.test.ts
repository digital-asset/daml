// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Template, Choice, ContractId } from "@daml/types";
import Ledger, {CreateEvent} from "./index";
import { Event } from "./index";
import * as jtv from "@mojotech/json-type-validation";
import type { EventEmitter } from 'events';
import mockConsole from "jest-mock-console";

const mockChange = jest.fn();
const mockConstructor = jest.fn();
const mockSend = jest.fn();
const mockFunctions = [mockChange, mockConstructor, mockSend];

type Foo = {key: string};
const fooKey = 'fooKey';

type Message =
  | { events: Event<Foo>[] }
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

  addEventListener(event: string, handler: (...args: unknown[]) => void) {
    this.eventEmitter.on(event, handler);
  }

  send(message: string) {
    mockSend(JSON.parse(message));
  }

  serverOpen() {
    this.eventEmitter.emit('open');
  }

  serverSend(message: Message) {
    this.eventEmitter.emit('message', {data: JSON.stringify(message)});
  }

  serverClose(event: {code: number; reason: string}) {
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

  test("receive empty events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([]);
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
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(null);
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
});
