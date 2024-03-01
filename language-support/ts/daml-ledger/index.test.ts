// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  Template,
  Choice,
  ChoiceFrom,
  ContractId,
  registerTemplate,
} from "@daml/types";
import Ledger, { CreateEvent } from "./index";
import { assert } from "./index";
import { Event } from "./index";
import * as jtv from "@mojotech/json-type-validation";
import type { EventEmitter } from "events";
import mockConsole from "jest-mock-console";

const mockLive = jest.fn();
const mockChange = jest.fn();
const mockConstructor = jest.fn();
const mockSend = jest.fn();
const mockClose = jest.fn();
const mockFunctions = [
  mockLive,
  mockChange,
  mockConstructor,
  mockSend,
  mockClose,
];

type Foo = { key: string };
const fooKey = "fooKey";

type Message =
  | { events: Event<Foo>[]; offset?: string | null }
  | { warnings: string[] }
  | { errors: string[] }
  | string; //for unexpected messages

interface MockWebSocket {
  serverOpen(): void;
  serverSend(message: Message): void;
  serverClose(event: { code: number; reason: string }): void;
}

let mockInstance = undefined as unknown as MockWebSocket;

jest.mock(
  "isomorphic-ws",
  () =>
    class {
      private eventEmitter: EventEmitter;
      //to represent readyState constants https://github.com/websockets/ws/blob/master/doc/ws.md#ready-state-constants
      public readyState: number;

      constructor(...args: unknown[]) {
        mockConstructor(...args);
        mockInstance = this;
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { EventEmitter } = require("events");
        this.eventEmitter = new EventEmitter();

        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { WsState } = require("./index");
        this.readyState = WsState.Connecting;
      }

      addEventListener(
        event: string,
        handler: (...args: unknown[]) => void,
      ): void {
        this.eventEmitter.on(event, handler);
      }

      send(message: string): void {
        mockSend(JSON.parse(message));
      }

      close(): void {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { WsState } = require("./index");
        this.readyState = WsState.Closed;
        mockClose();
      }

      serverOpen(): void {
        this.eventEmitter.emit("open");
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { WsState } = require("./index");
        this.readyState = WsState.Open;
      }

      serverSend(message: Message): void {
        this.eventEmitter.emit("message", { data: JSON.stringify(message) });
      }

      serverClose(event: { code: number; reason: string }): void {
        this.eventEmitter.emit("close", event);
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const { WsState } = require("./index");
        this.readyState = WsState.Closing;
      }
    },
);

const Foo: Template<Foo, string, "foo-id"> = {
  sdkVersion: "0.0.0-SDKVERSION",
  templateId: "foo-id",
  keyDecoder: jtv.string(),
  keyEncode: (s: string): unknown => s,
  decoder: jtv.object({ key: jtv.string() }),
  encode: o => o,
  // eslint-disable-next-line @typescript-eslint/ban-types
  Archive: {} as unknown as Choice<Foo, {}, {}, string> &
    ChoiceFrom<Template<Foo, string, "foo-id">>,
};

const fooCreateEvent = (
  coid: number,
  key?: string,
): CreateEvent<Foo, string, "foo-id"> => {
  return {
    templateId: "foo-id",
    contractId: coid.toString() as ContractId<Foo>,
    signatories: [],
    observers: [],
    key: key || fooKey,
    payload: { key: fooKey },
  };
};

const fooEvent = (coid: number): Event<Foo, string, "foo-id"> => {
  return { created: fooCreateEvent(coid), matchedQueries: [0] };
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

describe("internals", () => {
  test("assert throws as expected", () => {
    assert(true, "not thrown");
    expect(() => assert(false, "throws")).toThrow();
  });
});

describe("streamSubmit", () => {
  test("receive unknown message", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    expect(mockConstructor).toHaveBeenCalledTimes(1);
    expect(mockConstructor).toHaveBeenLastCalledWith(
      "ws://localhost:4000/v1/stream/query",
      ["jwt.token.dummyToken", "daml.ws.auth"],
    );
    stream.on("change", mockChange);

    mockInstance.serverOpen();
    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(mockSend).toHaveBeenLastCalledWith([
      { templateIds: [Foo.templateId] },
    ]);
    const restoreConsole = mockConsole();
    mockInstance.serverSend("mickey mouse");
    expect(console.error).toHaveBeenCalledWith(
      "Ledger.streamQuery unknown message",
      "mickey mouse",
    );
    restoreConsole();
  });

  test("receive warnings", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQueries(Foo, []);
    stream.on("change", mockChange);
    const restoreConsole = mockConsole();
    mockInstance.serverOpen();
    mockInstance.serverSend({ warnings: ["oh oh"] });
    expect(console.warn).toHaveBeenCalledWith("Ledger.streamQueries warnings", {
      warnings: ["oh oh"],
    });
    restoreConsole();
  });

  test("receive errors", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKey(Foo, fooKey);
    stream.on("change", mockChange);
    const restoreConsole = mockConsole();
    mockInstance.serverOpen();
    mockInstance.serverSend({ errors: ["not good!"] });
    expect(console.error).toHaveBeenCalledWith(
      "Ledger.streamFetchByKey errors",
      { errors: ["not good!"] },
    );
    restoreConsole();
  });

  test("receive null offset", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("live", mockLive);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [], offset: null });
    expect(mockLive).toHaveBeenCalledTimes(1);
    expect(mockLive).toHaveBeenLastCalledWith([]);
    expect(mockChange).not.toHaveBeenCalled();
  });

  test("reconnect on server close with appropriate offsets, when ws multiplexing is enabled", async () => {
    const reconnectThreshold = 200;
    const ledger = new Ledger({
      ...mockOptions,
      reconnectThreshold: reconnectThreshold,
      multiplexQueryStreams: true,
    });
    const stream = ledger.streamQuery(Foo, { key: "1" });
    stream.on("live", mockLive);
    stream.on("close", mockClose);
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [], offset: "4" });
    await new Promise(resolve => setTimeout(resolve, reconnectThreshold * 2));
    mockConstructor.mockClear();
    mockInstance.serverClose({ code: 1, reason: "test close" });
    expect(mockConstructor).toHaveBeenCalled();
    mockInstance.serverOpen();
    //first query with no offsets.
    expect(mockSend).toHaveBeenNthCalledWith(1, [
      { query: { key: "1" }, templateIds: ["foo-id"] },
    ]);
    //subsequent one on reconnection with offsets received
    expect(mockSend).toHaveBeenNthCalledWith(2, [
      { offset: "4", query: { key: "1" }, templateIds: ["foo-id"] },
    ]);
    mockSend.mockClear();
    mockConstructor.mockClear();

    // check that the client doesn't try to reconnect again.  it should only reconnect if it
    // received an event confirming the stream is live again, i.e. {events: [], offset: '4'}
    mockInstance.serverClose({ code: 1, reason: "test close" });
    expect(mockConstructor).not.toHaveBeenCalled();
  });

  test("reconnect on server close with appropriate offsets, when ws multiplexing is disabled", async () => {
    const reconnectThreshold = 200;
    const ledger = new Ledger({
      ...mockOptions,
      reconnectThreshold: reconnectThreshold,
      multiplexQueryStreams: false,
    });
    const stream = ledger.streamQuery(Foo);
    stream.on("live", mockLive);
    stream.on("close", mockClose);
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [], offset: "3" });
    await new Promise(resolve => setTimeout(resolve, reconnectThreshold * 2));
    mockConstructor.mockClear();
    mockInstance.serverClose({ code: 1, reason: "test close" });
    expect(mockConstructor).toHaveBeenCalled();
    mockInstance.serverOpen();
    expect(mockSend).toHaveBeenNthCalledWith(1, [{ templateIds: ["foo-id"] }]); //initial query
    expect(mockSend).toHaveBeenNthCalledWith(2, { offset: "3" }); // offsets sent when reconnecting.
    expect(mockSend).toHaveBeenNthCalledWith(3, [{ templateIds: ["foo-id"] }]); //reconnect query request.
    mockSend.mockClear();
    mockConstructor.mockClear();

    // check that the client doesn't try to reconnect again.  it should only reconnect if it
    // received an event confirming the stream is live again, i.e. {events: [], offset: '4'}
    mockInstance.serverClose({ code: 1, reason: "test close" });
    expect(mockConstructor).not.toHaveBeenCalled();
  });

  test("do not reconnect on client close", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    expect(mockConstructor).toHaveBeenCalled();
    mockConstructor.mockClear();
    stream.close();
    expect(mockConstructor).not.toHaveBeenCalled();
  });

  test("receive empty events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [] });
    expect(mockChange).toHaveBeenCalledTimes(0);
  });

  test("stop listening to a stream", () => {
    const ledger = new Ledger(mockOptions);
    registerTemplate(Foo as unknown as Template<Foo, unknown, string>);
    const stream = ledger.streamQuery(Foo);
    const count1 = jest.fn();
    const count2 = jest.fn();
    stream.on("change", count1);
    stream.on("change", count2);
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [1, 2, 3].map(fooEvent) });
    expect(count1).toHaveBeenCalledTimes(1);
    expect(count2).toHaveBeenCalledTimes(1);
    stream.off("change", count1);
    mockInstance.serverSend({ events: [1, 2, 3].map(fooEvent) });
    expect(count1).toHaveBeenCalledTimes(1);
    expect(count2).toHaveBeenCalledTimes(2);
  });
});

describe("streamQuery", () => {
  test("receive live event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("live", mockLive);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [fooEvent(1)], offset: "3" });
    expect(mockLive).toHaveBeenCalledTimes(1);
    expect(mockLive).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
  });

  test("receive one event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
  });

  test("receive several events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [1, 2, 3].map(fooEvent) });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(
      [1, 2, 3].map(cid => fooCreateEvent(cid)),
    );
  });

  test("drop matching created and archived events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQuery(Foo);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [fooEvent(1), fooEvent(2)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([
      fooCreateEvent(1),
      fooCreateEvent(2),
    ]);
    mockChange.mockClear();
    mockInstance.serverSend({ events: [fooArchiveEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(2)]);
  });
});

describe("streamQueries", () => {
  test("receive live event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQueries(Foo, []);
    stream.on("live", mockLive);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [fooEvent(1)], offset: "3" });
    expect(mockLive).toHaveBeenCalledTimes(1);
    expect(mockLive).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
  });

  test("receive one event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQueries(Foo, []);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenLastCalledWith([fooCreateEvent(1)]);
  });

  test("receive several events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQueries(Foo, []);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [1, 2, 3].map(fooEvent) });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith(
      [1, 2, 3].map(cid => fooCreateEvent(cid)),
    );
  });

  test("drop matching created and archived events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamQueries(Foo, []);
    stream.on("change", state => mockChange(state));
    mockInstance.serverOpen();
    mockInstance.serverSend({ events: [fooEvent(1), fooEvent(2)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([
      fooCreateEvent(1),
      fooCreateEvent(2),
    ]);
    mockChange.mockClear();
    mockInstance.serverSend({ events: [fooArchiveEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(2)]);
  });
});

describe("streamFetchByKey", () => {
  test("receive no event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKey(Foo, "badKey");
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
    mockInstance.serverSend({
      events: [fooEvent(1), fooEvent(2), fooEvent(3)],
    });
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

describe("streamFetchByKeys", () => {
  test("receive one event", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKeys(Foo, [fooKey]);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(1)]);
  });

  test("receive several events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKeys(Foo, [fooKey]);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({
      events: [fooEvent(1), fooEvent(2), fooEvent(3)],
    });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(3)]);
  });

  test("drop matching created and archived events", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKeys(Foo, [fooKey]);
    stream.on("change", state => mockChange(state));
    mockInstance.serverSend({ events: [fooEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([fooCreateEvent(1)]);
    mockChange.mockClear();
    mockInstance.serverSend({ events: [fooArchiveEvent(1)] });
    expect(mockChange).toHaveBeenCalledTimes(1);
    expect(mockChange).toHaveBeenCalledWith([null]);
  });

  test("watch multiple keys", () => {
    const create = (cid: number, key: string): Event<Foo> => ({
      created: fooCreateEvent(cid, key),
      matchedQueries: [0],
    });
    const archive = fooArchiveEvent;
    const send = (events: Event<Foo>[]): void =>
      mockInstance.serverSend({ events });
    const expectCids = (expected: (number | null)[]): void =>
      expect(mockChange).toHaveBeenCalledWith(
        expected.map((cid: number | null, idx) =>
          cid ? fooCreateEvent(cid, `key${idx + 1}`) : null,
        ),
      );

    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKeys(Foo, ["key1", "key2"]);
    stream.on("change", state => mockChange(state));

    send([create(1, "key1")]);
    expectCids([1, null]);

    send([create(2, "key2")]);
    expectCids([1, 2]);

    send([archive(1), create(3, "key1")]);
    expectCids([3, 2]);

    send([archive(2), archive(3), create(4, "key2")]);
    expectCids([null, 4]);
  });

  test("watch zero keys", () => {
    const ledger = new Ledger(mockOptions);
    const stream = ledger.streamFetchByKeys(Foo, []);
    stream.close();
    const change = jest.fn();
    stream.on("change", state => change(state));
    expect(change).toHaveBeenCalledTimes(1);
    expect(change).toHaveBeenCalledWith([]);
    mockInstance.serverSend({ events: [1, 2, 3].map(fooEvent) });
    expect(change).toHaveBeenCalledWith([]);
    expect(change).toHaveBeenCalledTimes(1);
  });
});
