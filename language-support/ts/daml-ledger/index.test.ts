// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import type { EventEmitter } from 'events';
import * as jtv from '@mojotech/json-type-validation';
import { Template, ContractId } from '@daml/types';
import Ledger, { Event, CreateEvent } from './index';

type Foo = Record<string, unknown>

const Foo = {
  templateId: 'FooTemplateId',
  decoder: () => jtv.object(),
  keyDecoder: () => jtv.constant(undefined),
} as unknown as Template<object>;


type Message =
  | { events: Event<Foo>[] }

interface MockWebSocket {
  serverOpen(): void;
  serverSend(message: Message): void;
  serverClose(event: {code: number; reason: string}): void;
}

let mockInstance = undefined as unknown as MockWebSocket;
const mockConstructor = jest.fn();
const mockSend = jest.fn();

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

const LEDGER_OPTIONS = {
  token: 'test_token',
  httpBaseUrl: 'http://localhost/',
};

test('basic test for Ledger.streamQuery', () => {
  const ledger = new Ledger(LEDGER_OPTIONS);
  const stream = ledger.streamQuery(Foo);

  const onChange = jest.fn();
  // NOTE(MH): For demonstrational purposes we ignore the second argument of
  // change event here.
  stream.on('change', state => onChange(state));
  const onClose = jest.fn();
  stream.on('close', onClose);


  expect(mockConstructor).toHaveBeenCalledTimes(1);
  expect(mockConstructor).toHaveBeenLastCalledWith(
    'ws://localhost/v1/stream/query',
    ['jwt.token.test_token', 'daml.ws.auth'],
  );

  mockInstance.serverOpen();
  expect(mockSend).toHaveBeenCalledTimes(1);
  expect(mockSend).toHaveBeenLastCalledWith({templateIds: [Foo.templateId]});

  const contract1: CreateEvent<Foo> = {
    templateId: Foo.templateId,
    contractId: '#1:1' as ContractId<Foo>,
    signatories: [],
    observers: [],
    agreementText: '',
    key: undefined,
    payload: {},
  };
  mockInstance.serverSend({events: [{created: contract1}]});
  expect(onChange).toHaveBeenCalledTimes(1);
  expect(onChange).toHaveBeenLastCalledWith([contract1]);
  onChange.mockClear();

  const contract2: CreateEvent<Foo> = {
    ...contract1,
    contractId: '#2:1' as ContractId<Foo>,
  };
  mockInstance.serverSend({events: [{created: contract2}]});
  expect(onChange).toHaveBeenCalledTimes(1);
  expect(onChange).toHaveBeenLastCalledWith([contract1, contract2]);

  const closeEvent = {code: 123, reason: 'bla'};
  mockInstance.serverClose(closeEvent);
  expect(onClose).toHaveBeenCalledTimes(1);
  expect(onClose).toHaveBeenLastCalledWith(closeEvent);
});
