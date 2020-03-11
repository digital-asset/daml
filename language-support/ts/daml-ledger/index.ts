// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { Choice, ContractId, List, Party, Template, Text, lookupTemplate } from '@daml/types';
import * as jtv from '@mojotech/json-type-validation';
import fetch from 'cross-fetch';
import { EventEmitter } from 'events';
import WebSocket from 'isomorphic-ws';

export type CreateEvent<T extends object, K = unknown, I extends string = string> = {
  templateId: I;
  contractId: ContractId<T>;
  signatories: List<Party>;
  observers: List<Party>;
  agreementText: Text;
  key: K;
  payload: T;
}

export type ArchiveEvent<T extends object, I extends string = string> = {
  templateId: I;
  contractId: ContractId<T>;
}

export type Event<T extends object, K = unknown, I extends string = string> =
  | { created: CreateEvent<T, K, I> }
  | { archived: ArchiveEvent<T, I> }

const decodeCreateEvent = <T extends object, K, I extends string>(template: Template<T, K, I>): jtv.Decoder<CreateEvent<T, K, I>> => jtv.object({
  templateId: jtv.constant(template.templateId),
  contractId: ContractId(template).decoder(),
  signatories: List(Party).decoder(),
  observers: List(Party).decoder(),
  agreementText: Text.decoder(),
  key: template.keyDecoder(),
  payload: template.decoder(),
});

const decodeCreateEventUnknown: jtv.Decoder<CreateEvent<object>> =
  jtv.valueAt(['templateId'], jtv.string()).andThen((templateId) =>
    decodeCreateEvent(lookupTemplate(templateId))
  );

const decodeArchiveEvent = <T extends object, K, I extends string>(template: Template<T, K, I>): jtv.Decoder<ArchiveEvent<T, I>> => jtv.object({
  templateId: jtv.constant(template.templateId),
  contractId: ContractId(template).decoder(),
});

const decodeArchiveEventUnknown: jtv.Decoder<ArchiveEvent<object>> =
  jtv.valueAt(['templateId'], jtv.string()).andThen(templateId =>
    decodeArchiveEvent(lookupTemplate(templateId))
  );

const decodeEvent = <T extends object, K, I extends string>(template: Template<T, K, I>): jtv.Decoder<Event<T, K, I>> => jtv.oneOf<Event<T, K, I>>(
  jtv.object({created: decodeCreateEvent(template)}),
  jtv.object({archived: decodeArchiveEvent(template)}),
);

const decodeEventUnknown: jtv.Decoder<Event<object>> = jtv.oneOf<Event<object>>(
  jtv.object({created: decodeCreateEventUnknown}),
  jtv.object({archived: decodeArchiveEventUnknown}),
);

async function decodeArchiveResponse<T extends object, K, I extends string>(
  template: Template<T, K, I>,
  archiveMethod: 'archive' | 'archiveByKey',
  archiveCommand: () => Promise<[{}, Event<object>[]]>,
): Promise<ArchiveEvent<T, I>> {
  // eslint-disable-next-line no-empty-pattern
  const [{}, events] = await archiveCommand();
  if (events.length === 1 && 'archived' in events[0] && events[0].archived.templateId === template.templateId) {
    return events[0].archived as ArchiveEvent<T, I>;
  } else {
    throw Error(`Ledger.${archiveMethod} is expected to cause one archive event for template ${template.templateId} \
      but caused ${JSON.stringify(events)}.`);
  }
}

function isRecordWith<Field extends string>(field: Field, x: unknown): x is Record<Field, unknown> {
  return typeof x === 'object' && x !== null && field in x;
}

/**
 * Type for queries against the `/v1/query` and `/v1/stream/query` endpoints
 * of the JSON API.
 * `Query<T>` is the type of queries that are valid when searching for
 * contracts of template type `T`.
 *
 * Comparison queries are not yet supported.
 *
 * NB: This type is heavily related to the `DeepPartial` type that can be found
 * in the TypeScript community.
 */
export type Query<T> = T extends object ? {[K in keyof T]?: Query<T[K]>} : T;
// TODO(MH): Support comparison queries.


type LedgerResponse = {
  status: number;
  result: unknown;
}

type LedgerError = {
  status: number;
  errors: string[];
}

const decodeLedgerResponse: jtv.Decoder<LedgerResponse> = jtv.object({
  status: jtv.number(),
  result: jtv.unknownJson(),
});

const decodeLedgerError: jtv.Decoder<LedgerError> = jtv.object({
  status: jtv.number(),
  errors: jtv.array(jtv.string()),
});

export type StreamCloseEvent = {
  code: number;
  reason: string;
}

/**
 * Interface for streams returned by the streaming methods of the `Ledger`
 * class. Each `'change'` event contains accumulated state of type `State` as
 * well as the ledger events that triggered the current state change.
 */
export interface Stream<T extends object, K, I extends string, State> {
  on(type: 'change', listener: (state: State, events: readonly Event<T, K, I>[]) => void): void;
  on(type: 'close', listener: (closeEvent: StreamCloseEvent) => void): void;
  off(type: 'change', listener: (state: State, events: readonly Event<T, K, I>[]) => void): void;
  off(type: 'close', listener: (closeEvent: StreamCloseEvent) => void): void;
  close(): void;
}

/**
 * Options for creating a handle to a DAML ledger.
 * - `token` JSON web token used for authentication.
 * - `httpBaseUrl` Optional base URL for the non-streaming endpoints of
 *   the JSON API. If this parameter is not provided, the protocol, host and
 *   port of the `window.location` object are used.
 * - `wsBaseUrl` Optional base URL for the streaming endpoints of the
 *   JSON API. If this parameter is not provided, the base URL for the
 *   non-streaming endpoints is used with the protocol 'http' or 'https'
 *   replaced by 'ws' or 'wss', respectively.
 *   Specifying this parameter explicitly can be useful when the
 *   non-streaming requests are proxied but the streaming request cannot be
 *   proxied, as it is the case with the development server of
 *   `create-react-app`.
 */
type LedgerOptions = {
  token: string;
  httpBaseUrl?: string;
  wsBaseUrl?: string;
}

/**
 * An object of type `Ledger` represents a handle to a DAML ledger.
 */
class Ledger {
  private readonly token: string;
  private readonly httpBaseUrl: string;
  private readonly wsBaseUrl: string;

  /**
   * Construct a new `Ledger` object.
   */
  constructor({token, httpBaseUrl, wsBaseUrl}: LedgerOptions) {
    if (!httpBaseUrl) {
      httpBaseUrl = `${window.location.protocol}//${window.location.host}/`;
    }
    if (!(httpBaseUrl.startsWith('http://') || httpBaseUrl.startsWith('https://'))) {
      throw Error(`Ledger: httpBaseUrl must start with 'http://' or 'https://'. (${httpBaseUrl})`);
    }
    if (!httpBaseUrl.endsWith('/')) {
      throw Error(`Ledger: httpBaseUrl must end with '/'. (${httpBaseUrl})`);
    }
    if (!wsBaseUrl) {
      wsBaseUrl = 'ws' + httpBaseUrl.slice(4);
    }
    if (!(wsBaseUrl.startsWith('ws://') || wsBaseUrl.startsWith('wss://'))) {
      throw Error(`Ledger: wsBaseUrl must start with 'ws://' or 'wss://'. (${wsBaseUrl})`);
    }
    if (!wsBaseUrl.endsWith('/')) {
      throw Error(`Ledger: wsBaseUrl must end with '/'. (${wsBaseUrl})`);
    }

    this.token = token;
    this.httpBaseUrl = httpBaseUrl;
    this.wsBaseUrl = wsBaseUrl;
  }

  /**
   * Internal function to submit a command to the JSON API.
   */
  private async submit(endpoint: string, payload: unknown): Promise<unknown> {
    const httpResponse = await fetch(this.httpBaseUrl + endpoint, {
      body: JSON.stringify(payload),
      headers: {
        'Authorization': 'Bearer ' + this.token,
        'Content-type': 'application/json'
      },
      method: 'post',
    });
    const json = await httpResponse.json();
    if (!httpResponse.ok) {
      console.log(json);
      throw jtv.Result.withException(decodeLedgerError.run(json));
    }
    const ledgerResponse = jtv.Result.withException(decodeLedgerResponse.run(json));
    return ledgerResponse.result;
  }

  /**
   * Retrieve contracts for a given template. When no `query` argument is
   * given, all contracts visible to the submitting party are returned. When a
   * `query` argument is given, only those contracts matching the query are
   * returned. See https://docs.daml.com/json-api/search-query-language.html
   * for a description of the query language.
   */
  async query<T extends object, K, I extends string>(template: Template<T, K, I>, query?: Query<T>): Promise<CreateEvent<T, K, I>[]> {
    const payload = {templateIds: [template.templateId], query};
    const json = await this.submit('v1/query', payload);
    return jtv.Result.withException(jtv.array(decodeCreateEvent(template)).run(json));
  }

  /**
   * @deprecated since version 0.13.51. Use `Ledger.query` wihtout a `query`
   * argument instead.
   */
  async fetchAll<T extends object, K, I extends string>(template: Template<T, K, I>): Promise<CreateEvent<T, K, I>[]> {
    return this.query(template, {} as Query<T>);
  }

  /**
   * Fetch a contract identified by its contract ID.
   */
  async fetch<T extends object, K, I extends string>(template: Template<T, K, I>, contractId: ContractId<T>): Promise<CreateEvent<T, K, I> | null> {
    const payload = {
      templateId: template.templateId,
      contractId,
    };
    const json = await this.submit('v1/fetch', payload);
    return jtv.Result.withException(jtv.oneOf(jtv.constant(null), decodeCreateEvent(template)).run(json));
  }

  /**
   * Fetch a contract identified by its contract key.
   */
  async fetchByKey<T extends object, K, I extends string>(template: Template<T, K, I>, key: K): Promise<CreateEvent<T, K, I> | null> {
    if (key === undefined) {
      throw Error(`Cannot lookup by key on template ${template.templateId} because it does not define a key.`);
    }
    const payload = {
      templateId: template.templateId,
      key,
    };
    const json = await this.submit('v1/fetch', payload);
    return jtv.Result.withException(jtv.oneOf(jtv.constant(null), decodeCreateEvent(template)).run(json));
  }

  /**
   * @deprecated since version 0.13.51. Use `Ledger.fetchByKey` instead.
   */
  async lookupByKey<T extends object, K, I extends string>(template: Template<T, K, I>, key: K): Promise<CreateEvent<T, K, I> | null> {
    return this.fetchByKey(template, key);
  }

  /**
   * Create a contract for a given template.
   */
  async create<T extends object, K, I extends string>(template: Template<T, K, I>, payload: T): Promise<CreateEvent<T, K, I>> {
    const command = {
      templateId: template.templateId,
      payload,
    };
    const json = await this.submit('v1/create', command);
    return jtv.Result.withException(decodeCreateEvent(template).run(json));
  }

  /**
   * Exercise a choice on a contract identified by its contract ID.
   */
  async exercise<T extends object, C, R>(choice: Choice<T, C, R>, contractId: ContractId<T>, argument: C): Promise<[R , Event<object>[]]> {
    const payload = {
      templateId: choice.template().templateId,
      contractId,
      choice: choice.choiceName,
      argument,
    };
    const json = await this.submit('v1/exercise', payload);
    // Decode the server response into a tuple.
    const responseDecoder: jtv.Decoder<{exerciseResult: R; events: Event<object>[]}> = jtv.object({
      exerciseResult: choice.resultDecoder(),
      events: jtv.array(decodeEventUnknown),
    });
    const {exerciseResult, events} = jtv.Result.withException(responseDecoder.run(json));
    return [exerciseResult, events];
  }

  /**
   * Exercise a choice on a contract identified by its contract key.
   */
  async exerciseByKey<T extends object, C, R, K>(choice: Choice<T, C, R, K>, key: K, argument: C): Promise<[R, Event<object>[]]> {
    if (key === undefined) {
      throw Error(`Cannot exercise by key on template ${choice.template().templateId} because it does not define a key.`);
    }
    const payload = {
      templateId: choice.template().templateId,
      key,
      choice: choice.choiceName,
      argument,
    };
    const json = await this.submit('v1/exercise', payload);
    // Decode the server response into a tuple.
    const responseDecoder: jtv.Decoder<{exerciseResult: R; events: Event<object>[]}> = jtv.object({
      exerciseResult: choice.resultDecoder(),
      events: jtv.array(decodeEventUnknown),
    });
    const {exerciseResult, events} = jtv.Result.withException(responseDecoder.run(json));
    return [exerciseResult, events];
  }

  /**
   * Archive a contract identified by its contract ID.
   */
  async archive<T extends object, K, I extends string>(template: Template<T, K, I>, contractId: ContractId<T>): Promise<ArchiveEvent<T, I>> {
    return decodeArchiveResponse(template, 'archive', () => this.exercise(template.Archive, contractId, {}));
  }

  /**
   * Archive a contract identified by its contract key.
   */
  async archiveByKey<T extends object, K, I extends string>(template: Template<T, K, I>, key: K): Promise<ArchiveEvent<T, I>> {
    return decodeArchiveResponse(template, 'archiveByKey', () => this.exerciseByKey(template.Archive, key, {}));
  }

  /**
   * Internal command to submit a request to a streaming endpoint of the
   * JSON API. Returns a stream consisting of accumulated state together with
   * the events that produced the latest state change. The `change` function
   * must be an operation of the monoid `Event<T, K, I>[]` on the set `State`,
   * i.e., for all `s: State` and `x, y: Event<T, K, I>[]` we
   * must have the structural equalities
   * ```
   * change(s, []) == s
   * change(s, x.concat(y)) == change(change(s, x), y)
   * ```
   * Also, `change` must never change its arguments.
   */
  private streamSubmit<T extends object, K, I extends string, State>(
    template: Template<T, K, I>,
    endpoint: string,
    request: unknown,
    init: State,
    change: (state: State, events: readonly Event<T, K, I>[]) => State,
  ): Stream<T, K, I, State> {
    const protocols = ['jwt.token.' + this.token, 'daml.ws.auth'];
    const ws = new WebSocket(this.wsBaseUrl + endpoint, protocols);
    let haveSeenEvents = false;
    let state = init;
    const emitter = new EventEmitter();
    ws.onopen = () => {
      ws.send(JSON.stringify(request));
    };
    ws.onmessage = event => {
      const json: unknown = JSON.parse(event.data.toString());
      if (isRecordWith('events', json)) {
        const events = jtv.Result.withException(jtv.array(decodeEvent(template)).run(json.events));
        if (events.length == 0 && isRecordWith('offset', json)) {
          if (!haveSeenEvents) {
            // NOTE(MH): If we receive the marker indicating that we are switching
            // from the ACS to the live stream and we haven't received any events
            // yet, we signal this by pretending we received an empty list of
            // events. This never does any harm.
            haveSeenEvents = true;
            emitter.emit('change', state, []);
          }
        } else {
          state = change(state, events);
          if (isRecordWith('offset', json)) {
            haveSeenEvents = true;
          }
          emitter.emit('change', state, events);
        }
      } else if (isRecordWith('warnings', json)) {
        console.warn('Ledger.streamQuery warnings', json);
      } else if (isRecordWith('errors', json)) {
        console.error('Ledger.streamQuery errors', json);
      } else {
        console.error('Ledger.streamQuery unknown message', json);
      }
    };
    // NOTE(MH): We ignore the 'error' event since it is always followed by a
    // 'close' event, which we need to handle anyway.
    ws.onclose = ({code, reason}) => {
      emitter.emit('close', {code, reason});
    };
    // TODO(MH): Make types stricter.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const on = (type: string, listener: any) => emitter.on(type, listener);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const off = (type: string, listener: any) => emitter.on(type, listener);
    const close = () => {
      emitter.removeAllListeners();
      ws.close();
    };
    return {on, off, close};
  }

  /**
   * Retrieve a consolidated stream of events for a given template and query.
   * The accumulated state is the current set of active contracts matching the
   * query.
   * When no `query` argument is
   * given, all events visible to the submitting party are returned. When a
   * `query` argument is given, only those create events matching the query are
   * returned. See https://docs.daml.com/json-api/search-query-language.html
   * for a description of the query language.
   */
  streamQuery<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    query?: Query<T>,
  ): Stream<T, K, I, readonly CreateEvent<T, K, I>[]> {
    const request = {templateIds: [template.templateId], query};
    const change = (contracts: readonly CreateEvent<T, K, I>[], events: readonly Event<T, K, I>[]) => {
      const archiveEvents: Set<ContractId<T>> = new Set();
      const createEvents: CreateEvent<T, K, I>[] = [];
      for (const event of events) {
        if ('created' in event) {
          createEvents.push(event.created);
        } else { // i.e. 'archived' in event
          archiveEvents.add(event.archived.contractId);
        }
      }
      return contracts
        .concat(createEvents)
        .filter(contract => !archiveEvents.has(contract.contractId));
    };
    return this.streamSubmit(template, 'v1/stream/query', request, [], change);
  }

  streamFetchByKey<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    key: K,
  ): Stream<T, K, I, CreateEvent<T, K, I> | null> {
    const request = [{templateId: template.templateId, key}];
    const change = (contract: CreateEvent<T, K, I> | null, events: readonly Event<T, K, I>[]) => {
      // NOTE(MH, #4564): We're very lenient here. We should not see a create
      // event when `contract` is currently not null. We should also only see
      // archive events when `contract` is currently not null and the contract
      // ids match. However, the JSON API does not provied these guarantees yet
      // but we're working on them.
      for (const event of events) {
        if ('created' in event) {
          contract = event.created;
        } else { // i.e. 'archived' in event
          if (contract && contract.contractId === event.archived.contractId) {
            contract = null;
          }
        }
      }
      return contract;
    }
    return this.streamSubmit(template, 'v1/stream/fetch', request, null, change);
  }
}

export default Ledger;
