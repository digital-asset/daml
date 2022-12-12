// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import {
  Choice,
  ChoiceFrom,
  ContractId,
  List,
  Party,
  Template,
  TemplateOrInterface,
  Text,
  lookupTemplate,
} from "@daml/types";
import * as jtv from "@mojotech/json-type-validation";
import fetch from "cross-fetch";
import { EventEmitter } from "events";
import WebSocket from "isomorphic-ws";
import _, { isUndefined } from "lodash";

/**
 * The result of a ``query`` against the ledger.
 *
 * Note: this is meant to be used by @daml/react.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 */
export type QueryResult<T extends object, K, I extends string> = {
  /** Contracts matching the query. */
  contracts: readonly CreateEvent<T, K, I>[];
  /** Indicator for whether the query is executing. */
  loading: boolean;
};

/**
 * Full information about a Party.
 *
 */
export type PartyInfo = {
  identifier: Party;
  displayName?: string;
  isLocal: boolean;
};

const partyInfoDecoder: jtv.Decoder<PartyInfo> = jtv.object({
  identifier: jtv.string(),
  displayName: jtv.optional(jtv.string()),
  isLocal: jtv.boolean(),
});

export type User = {
  userId: string;
  primaryParty?: Party;
};

const userDecoder: jtv.Decoder<User> = jtv.object({
  userId: jtv.string(),
  primaryParty: jtv.optional(jtv.string()),
});

export type CanActAs = {
  type: "CanActAs";
  party: string;
};

export type CanReadAs = {
  type: "CanReadAs";
  party: string;
};

export type ParticipantAdmin = {
  type: "ParticipantAdmin";
};

export type UserRight = CanActAs | CanReadAs | ParticipantAdmin;

export class UserRightHelper {
  static canActAs(party: string): UserRight {
    return { type: "CanActAs", party: party };
  }

  static canReadAs(party: string): UserRight {
    return { type: "CanReadAs", party: party };
  }

  static participantAdmin: UserRight = {
    type: "ParticipantAdmin",
  };
}

const userRightDecoder: jtv.Decoder<UserRight> = jtv.oneOf<UserRight>(
  jtv.object<CanActAs>({
    type: jtv.constant("CanActAs"),
    party: jtv.string(),
  }),
  jtv.object<CanReadAs>({
    type: jtv.constant("CanReadAs"),
    party: jtv.string(),
  }),
  jtv.object<ParticipantAdmin>({
    type: jtv.constant("ParticipantAdmin"),
  }),
);

export type PackageId = string;

const decode = <R>(decoder: jtv.Decoder<R>, data: unknown): R => {
  return jtv.Result.withException(decoder.run(data));
};

/**
 * A newly created contract.
 *
 * @typeparam T The contract payload type.
 * @typeparam K The contract key type.
 * @typeparam I The contract type id.
 *
 */
export type CreateEvent<
  T extends object,
  K = unknown,
  I extends string = string,
> = {
  templateId: I;
  contractId: ContractId<T>;
  signatories: List<Party>;
  observers: List<Party>;
  agreementText: Text;
  key: K;
  payload: T;
};

/**
 * An archived contract.
 *
 * @typeparam T The contract template or interface type.
 * @typeparam I The template or interface id.
 */
export type ArchiveEvent<T extends object, I extends string = string> = {
  templateId: I;
  contractId: ContractId<T>;
};

/**
 * An event is either the creation or archival of a contract.
 *
 * @typeparam T The contract template type.
 * @typeparam K The contract key type.
 * @typeparam I The contract id type.
 */
export type Event<T extends object, K = unknown, I extends string = string> =
  | { created: CreateEvent<T, K, I>; matchedQueries: number[] }
  | { created: CreateEvent<T, K, I> }
  | { archived: ArchiveEvent<T, I> };

function lookupTemplateOrUnknownInterface(
  templateId: string,
): TemplateOrInterface<object, unknown> {
  try {
    return lookupTemplate(templateId);
  } catch (e) {
    if (e instanceof Error)
      return {
        templateId,
        sdkVersion: "0.0.0-SDKVERSION",
        // there is no way to properly decode in this case, so we
        // discard the data instead.  #15042
        decoder: jtv.succeed({}),
        keyDecoder: jtv.succeed(undefined),
      };
    else throw e;
  }
}

/**
 * Decoder for a [[CreateEvent]].
 */
const decodeCreateEvent = <T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
): jtv.Decoder<CreateEvent<T, K, I>> =>
  jtv.object({
    templateId: jtv.constant(template.templateId),
    contractId: ContractId(template).decoder,
    signatories: List(Party).decoder,
    observers: List(Party).decoder,
    agreementText: Text.decoder,
    key: template.keyDecoder,
    payload: template.decoder,
  });

/**
 * Decoder for a [[CreateEvent]] of unknown contract template.
 */
const decodeCreateEventUnknown: jtv.Decoder<CreateEvent<object>> = jtv
  .valueAt(["templateId"], jtv.string())
  .andThen(templateId =>
    decodeCreateEvent(lookupTemplateOrUnknownInterface(templateId)),
  );

/**
 * Decoder for an [[ArchiveEvent]].
 */
const decodeArchiveEvent = <T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
): jtv.Decoder<ArchiveEvent<T, I>> =>
  jtv.object({
    templateId: jtv.constant(template.templateId),
    contractId: ContractId(template).decoder,
  });

/**
 * Decoder for an [[ArchiveEvent]] of unknown contract template.
 */
const decodeArchiveEventUnknown: jtv.Decoder<ArchiveEvent<object>> = jtv
  .valueAt(["templateId"], jtv.string())
  .andThen(templateId =>
    decodeArchiveEvent(lookupTemplateOrUnknownInterface(templateId)),
  );

/**
 * Decoder for an [[Event]].
 */
const decodeEvent = <T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
): jtv.Decoder<Event<T, K, I>> =>
  jtv.oneOf<Event<T, K, I>>(
    jtv.object({
      created: decodeCreateEvent(template),
      matchedQueries: jtv.array(jtv.number()),
    }),
    jtv.object({ created: decodeCreateEvent(template) }),
    jtv.object({ archived: decodeArchiveEvent(template) }),
  );

/**
 * Decoder for an [[Event]] with unknown contract template.
 */
const decodeEventUnknown: jtv.Decoder<Event<object>> = jtv.oneOf<Event<object>>(
  jtv.object({
    created: decodeCreateEventUnknown,
    matchedQueries: jtv.array(jtv.number()),
  }),
  jtv.object({ created: decodeCreateEventUnknown }),
  jtv.object({ archived: decodeArchiveEventUnknown }),
);

/**
 * @internal
 */
async function decodeArchiveResponse<T extends object, K, I extends string>(
  template: Template<T, K, I>,
  archiveMethod: "archive" | "archiveByKey",
  // eslint-disable-next-line @typescript-eslint/ban-types
  archiveCommand: () => Promise<[{}, Event<object>[]]>,
): Promise<ArchiveEvent<T, I>> {
  // eslint-disable-next-line no-empty-pattern
  const [{}, events] = await archiveCommand();
  if (
    events.length === 1 &&
    "archived" in events[0] &&
    events[0].archived.templateId === template.templateId
  ) {
    return events[0].archived as ArchiveEvent<T, I>;
  } else {
    throw Error(`Ledger.${archiveMethod} is expected to cause one archive event for template ${
      template.templateId
    } \
      but caused ${JSON.stringify(events)}.`);
  }
}

/**
 * @internal
 */
function isRecordWith<Field extends string>(
  field: Field,
  x: unknown,
): x is Record<Field, unknown> {
  return typeof x === "object" && x !== null && field in x;
}

/**
 *
 * @internal
 */
function isCreateWithMatchedQueries<
  T extends object,
  K = unknown,
  I extends string = string,
>(
  event: Event<T, K, I>,
): event is { created: CreateEvent<T, K, I>; matchedQueries: number[] } {
  return isRecordWith("created", event);
}

/** @internal
 * exported for testing only
 */
export function assert(b: boolean, m: string): void {
  if (!b) {
    throw m;
  }
}

/**
 * `Query<T>` is the type of queries for searching for contracts of template
 * or interface type `T`.
 *
 * `Query<T>` is an object consisting of a subset of the fields of `T`.
 *
 * Comparison queries are not yet supported.
 *
 * NB: This type is heavily related to the `DeepPartial` type that can be found
 * in the TypeScript community.
 *
 * @typeparam T The contract template type.
 *
 */
export type Query<T> = T extends object ? { [K in keyof T]?: Query<T[K]> } : T;
// TODO(MH): Support comparison queries.

/** @internal
 *
 * Official documentation (docs/source/json-api/search-query-language.rst)
 * currently explicitly forbids the use of lists, textmaps and genmaps in
 * queries. As long as that restriction stays, there is no need for any kind of
 * encoding here.
 */
function encodeQuery<T extends object, K, I extends string>(
  _template: TemplateOrInterface<T, K, I>,
  query?: Query<T>,
): unknown {
  return query;
}

/**
 * Status code and result returned by a call to the ledger.
 */
type LedgerResponse = {
  status: number;
  result: unknown;
  warnings: unknown | undefined;
};

/**
 * Error code and messages returned by the ledger.
 */
type LedgerError = {
  status: number;
  errors: string[];
  warnings: unknown | undefined;
};

/**
 * @internal
 */
const decodeLedgerResponse: jtv.Decoder<LedgerResponse> = jtv.object({
  status: jtv.number(),
  result: jtv.unknownJson(),
  warnings: jtv.optional(jtv.unknownJson()),
});

/**
 * @internal
 */
const decodeLedgerError: jtv.Decoder<LedgerError> = jtv.object({
  status: jtv.number(),
  errors: jtv.array(jtv.string()),
  warnings: jtv.optional(jtv.unknownJson()),
});

/**
 * Event emitted when a stream gets closed.
 */
export type StreamCloseEvent = {
  code: number;
  reason: string;
};

/**
 * Interface for streams returned by the streaming methods of the `Ledger`
 * class. Each `'change'` event contains accumulated state of type `State` as
 * well as the ledger events that triggered the current state change.
 *
 * @typeparam T The contract template type.
 * @typeparam K The contract key type.
 * @typeparam I The contract id type.
 * @typeparam State The accumulated state.
 */
export interface Stream<T extends object, K, I extends string, State> {
  /**
   * Register a callback that will be called when the state of the stream has
   * caught up with the Active Contract Set and is now receiving new transactions.
   * @param type 'live'
   * @param listener function taking the state of the stream as an argument.
   */
  on(type: "live", listener: (state: State) => void): void;
  /**
   * Register a callback that will be called when the state of the stream changes,
   * eg. new contract creates or archives.
   * @param type 'change'
   * @param listener function taking the state of the stream and new events as
   * arguments.
   */
  on(
    type: "change",
    listener: (state: State, events: readonly Event<T, K, I>[]) => void,
  ): void;
  /**
   * Register a callback that will be called when the underlying stream is closed.
   * @param type 'close'
   * @param listener a function taking a StreamCloseEvent as an argument.
   */
  on(type: "close", listener: (closeEvent: StreamCloseEvent) => void): void;
  /**
   * Remove the registered callback for the 'live' event.
   * @param type 'live'
   * @param listener function to be deregistered.
   */
  off(type: "live", listener: (state: State) => void): void;
  /**
   * Remove the registered callback for the 'change' event.
   * @param type 'change'
   * @param listener function to be deregistered.
   */
  off(
    type: "change",
    listener: (state: State, events: readonly Event<T, K, I>[]) => void,
  ): void;
  /**
   * Remove the registered callback for the 'close' event.
   * @param type 'close'
   * @param listener function to be deregistered.
   */
  off(type: "close", listener: (closeEvent: StreamCloseEvent) => void): void;
  /**
   * Close the Stream and stop receiving events.
   */
  close(): void;
}

/**
 * Options for creating a handle to a Daml ledger.
 */
export type LedgerOptions = {
  /** JSON web token used for authentication. */
  token: string;
  /**
   * Optional base URL for the non-streaming endpoints of the JSON API. If this parameter is not
   * provided, the protocol, host and port of the `window.location` object are used.
   */
  httpBaseUrl?: string;
  /**
   * Optional base URL for the streaming endpoints of the JSON API. If this parameter is not
   * provided, the base URL for the non-streaming endpoints is used with the protocol 'http' or
   * 'https' replaced by 'ws' or 'wss', respectively.  Specifying this parameter explicitly can be
   * useful when the non-streaming requests are proxied but the streaming request cannot be proxied,
   * as it is the case with the development server of `create-react-app`.
   */
  wsBaseUrl?: string;
  /**
   * Optional number of milliseconds a connection has to be live to be considered healthy. If the
   * connection is closed after being live for at least this amount of time, the `Ledger` tries to
   * reconnect, else not.
   */
  reconnectThreshold?: number;

  /**
   * Optional to enable/disable feature of all streaming request to the query endpoint being multiplexed
   * through a single web socket, is enabled by default.
   */
  multiplexQueryStreams?: boolean;
};

class StreamEventEmitter<T extends object, K, I extends string, State>
  extends EventEmitter
  implements Stream<T, K, I, State>
{
  /** Passed at construction time, called _before_ the 'close' event is emitted */
  private readonly beforeClosing: () => void;

  constructor({ beforeClosing }: { beforeClosing: () => void }) {
    super();
    this.beforeClosing = beforeClosing;
  }

  close(): void {
    this.beforeClosing();
    this.emit("close", { code: 4000, reason: "called .close()" });
    this.removeAllListeners();
  }
}

type QueryResponseStream<
  T extends object,
  K,
  I extends string,
> = StreamEventEmitter<T, K, I, readonly CreateEvent<T, K, I>[]>;

const NoOffsetReceivedYet = Symbol("NoOffsetReceivedYet");
const NullOffsetReceived = Symbol("NullOffsetReceived");

type StreamingQuery<T extends object, K, I extends string> = {
  template: TemplateOrInterface<T, K, I>;
  queries: Query<T>[];
  stream: QueryResponseStream<T, K, I>;
  offset: string | typeof NoOffsetReceivedYet | typeof NullOffsetReceived;
  state: Map<ContractId<T>, CreateEvent<T, K, I>>; // all JavaScript Map iterators preserve insertion order
  caller: string;
};

type StreamingQueryRequest = {
  templateIds: string[];
  query?: object;
  offset?: string;
};

function append<K, V>(map: Map<K, V[]>, key: K, value: V): void {
  if (map.has(key)) {
    map.get(key)?.push(value);
  } else {
    map.set(key, [value]);
  }
}

/**
 * @deprecated All usages of this function should be replaced by just
 *             iterating over the iterator. For this to happen, the TS
 *             compiler requires the --downlevelIteration flag, which
 *             however does not play nicely with Jest when running the
 *             tests.
 *
 * TODO Moving the compilation target to ES6 probably would solve this, investigate.
 */
function materialize<A>(iterator: IterableIterator<A>): Array<A> {
  return Array.from(iterator);
}

export enum WsState {
  Connecting = 0,
  Open = 1,
  Closing = 2,
  Closed = 3,
}

/**
 * @internal
 *
 * A special handler for stream requests to the /v1/stream/query endpoint.
 * The query endpoint supports providing offsets on a per-query basis.
 * This class leverages this feature by multiplexing virtual streaming requests to a single web socket.
 */
class QueryStreamsManager {
  private static readonly ENDPOINT: string = "v1/stream/query";

  // Mutable state BEGIN

  // Ongoing streaming queries that will be the downstream consumers of web socket messages
  private readonly queries: Set<StreamingQuery<object, unknown, string>> =
    new Set();

  // Lookup tables used to route events to the relevant consumers:
  //  - consumers for create events can be looked up based on their match index
  //    - store the offset by which a match indexes needs to be shifted before the event is passed to the consumer
  //  - archive events can be lookup up by template identifier
  //    - this causes the consumer to observe what is known as "phantom archives", which are known and documented
  private matchIndexLookupTable: [
    StreamingQuery<object, unknown, string>,
    number,
  ][] = [];
  private templateIdsLookupTable: {
    [templateId: string]: Set<StreamingQuery<object, unknown, string>>;
  } = {};

  // Accumulates each query in a flattened form to be sent as a single request to the JSON API
  private request: StreamingQueryRequest[] = [];

  // web socket handle and associated properties
  private ws?: WebSocket;
  private wsLiveSince?: number;

  // to track changes on web socket queries
  private wsQueriesChange: boolean = false;

  // Mutable state END

  private readonly protocols: string[];
  private readonly url: string;
  private readonly reconnectThresholdMs: number;

  private static toRequest(
    query: StreamingQuery<object, unknown, string>,
  ): StreamingQueryRequest[] {
    const request: StreamingQueryRequest[] =
      query.queries.length == 0
        ? [{ templateIds: [query.template.templateId] }]
        : query.queries.map(q => ({
            templateIds: [query.template.templateId],
            query: encodeQuery(query.template, q) as object,
          }));
    if (typeof query.offset === "string") {
      for (const r of request) {
        r.offset = query.offset;
      }
    }
    return request;
  }

  private resetAllState(): void {
    // close ws if defined
    if (this.ws !== undefined) {
      this.ws.close();
    }
    this.queries.clear();
    this.matchIndexLookupTable = [];
    this.templateIdsLookupTable = {};
    this.request = [];
    this.ws = undefined;
    this.wsLiveSince = undefined;
    this.wsQueriesChange = false;
  }

  private handleQueriesChange(): void {
    //eslint-disable-next-line @typescript-eslint/no-this-alias
    const manager = this; // stable self-reference for callbacks

    if (manager.queries.size > 0) {
      if (manager.ws !== undefined) {
        //set the queries change flag to true, this should eventually get reset once the ws is closed.
        manager.wsQueriesChange = true;
        manager.wsLiveSince = undefined;
        manager.ws.close();
        manager.ws = undefined;
      }

      const ws = new WebSocket(manager.url, manager.protocols);
      const onWsMessage =
        (ws: WebSocket) =>
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ({ data }: { data: any }): void => {
          if (ws.readyState === WsState.Open) {
            const json: unknown = JSON.parse(data.toString());
            if (isRecordWith("events", json)) {
              const events: Event<object>[] = jtv.Result.withException(
                jtv.array(decodeEventUnknown).run(json.events),
              );
              const multiplexer: Map<
                StreamingQuery<object, unknown, string>,
                Event<object>[]
              > = new Map();
              for (const event of events) {
                if (isCreateWithMatchedQueries<object>(event)) {
                  const consumersToMatchedQueries: Map<
                    StreamingQuery<object, unknown, string>,
                    number[]
                  > = new Map();
                  for (const matchIndex of event.matchedQueries) {
                    const [consumer, matchIndexOffset] =
                      manager.matchIndexLookupTable[matchIndex];
                    append(
                      consumersToMatchedQueries,
                      consumer,
                      matchIndexOffset,
                    );
                  }
                  for (const [consumer, matchedQueries] of materialize(
                    consumersToMatchedQueries.entries(),
                  )) {
                    // Create a new copy of the event for each consumer to freely mangle the matched queries and avoid sharing mutable state
                    append(multiplexer, consumer, { ...event, matchedQueries });
                  }
                } else if ("archived" in event) {
                  const consumers =
                    manager.templateIdsLookupTable[event.archived.templateId];
                  for (const consumer of materialize(consumers.values())) {
                    // Create a new copy of the event for each consumer to avoid sharing mutable state
                    append(multiplexer, consumer, { ...event });
                  }
                } else {
                  console.error(
                    `${event} unknown event type received, expected created with matchedQueries or archived`,
                    json,
                  );
                }
              }
              for (const [consumer, events] of materialize(
                multiplexer.entries(),
              )) {
                for (const event of events) {
                  if (isCreateWithMatchedQueries<object>(event)) {
                    consumer.state.set(event.created.contractId, event.created);
                  } else if ("archived" in event) {
                    consumer.state.delete(event.archived.contractId);
                  }
                }
                consumer.stream.emit(
                  "change",
                  Array.from(consumer.state.values()),
                  events,
                );
              }

              if (isRecordWith("offset", json)) {
                const offset = jtv.Result.withException(
                  jtv.oneOf(jtv.constant(null), jtv.string()).run(json.offset),
                );
                if (manager.wsLiveSince === undefined) {
                  //on receiving the first offset event we consider the web socket to be live.
                  manager.wsLiveSince = Date.now();
                }
                for (const consumer of materialize(manager.queries.values())) {
                  if (!(typeof consumer.offset === "string")) {
                    // Rebuilding the state array from scratch to make sure mutable state is not shared between the 'change' and 'live' event
                    consumer.stream.emit(
                      "live",
                      Array.from(consumer.state.values()),
                    );
                  }
                  if (typeof offset === "string") {
                    consumer.offset = offset;
                  } else {
                    consumer.offset = NullOffsetReceived;
                  }
                }
              }
            } else if (isRecordWith("warnings", json)) {
              for (const query of materialize(manager.queries.values())) {
                console.warn(`${query.caller} warnings`, json);
              }
            } else if (isRecordWith("errors", json)) {
              for (const query of materialize(manager.queries.values())) {
                console.warn(`${query.caller} errors`, json);
              }
            } else {
              for (const query of materialize(manager.queries.values())) {
                console.error(`${query.caller} unknown message`, json);
              }
            }
          }
        };

      const onWsOpen = (): void => {
        // only make a new websocket request if we have registered queries
        if (manager.queries.size > 0) {
          let newRequests: StreamingQueryRequest[] = [];
          let newMatchIndexLookupTable: [
            StreamingQuery<object, unknown, string>,
            number,
          ][] = [];
          for (const query of materialize(manager.queries.values())) {
            const request = QueryStreamsManager.toRequest(query);

            // Add entries to the lookup table for create events
            const matchIndexOffset = newMatchIndexLookupTable.length;
            const matchIndexLookupTableEntries = new Array(request.length).fill(
              [query, matchIndexOffset],
            );
            newMatchIndexLookupTable = newMatchIndexLookupTable.concat(
              matchIndexLookupTableEntries,
            );

            // Add entries to the lookup table for archive events
            for (const { templateIds } of request) {
              for (const templateId of templateIds) {
                manager.templateIdsLookupTable[templateId] =
                  manager.templateIdsLookupTable[templateId] || new Set();
                manager.templateIdsLookupTable[templateId].add(query);
              }
            }

            //since we go through all queries on the manager, we should be safely able to rebuild the whole request
            newRequests = newRequests.concat(request);
          }
          manager.request = newRequests;
          manager.matchIndexLookupTable = newMatchIndexLookupTable;
          manager.ws?.send(
            JSON.stringify(Array.from(manager.request.values())),
          );
        }
      };

      const onWsClose = (): void => {
        //if not a web socket queries change then we need to initiate reconnect logic.
        if (!manager.wsQueriesChange) {
          // The web socket has been closed due to an error
          // If the conditions are met, attempt to reconnect and/or inform downstream consumers
          const now = Date.now();
          if (
            manager.wsLiveSince !== undefined &&
            now - manager.wsLiveSince >= manager.reconnectThresholdMs
          ) {
            console.log(
              `Reconnecting ws, previously liveSince: ${manager.wsLiveSince} and reconnectThresholdMs: ${manager.reconnectThresholdMs}`,
            );
            manager.wsLiveSince = undefined;
            const ws = new WebSocket(manager.url, manager.protocols);
            ws.addEventListener("open", onWsOpen);
            ws.addEventListener("message", onWsMessage(ws));
            ws.addEventListener("close", onWsClose);
            manager.ws = ws;
          } else {
            // ws has closed too quickly / never managed to connect: we give up
            for (const consumer of materialize(manager.queries.values())) {
              consumer.stream.emit("close", {
                code: 4001,
                reason: "ws connection failed",
              });
              consumer.stream.removeAllListeners();
            }
            manager.resetAllState();
          }
        } else {
          //this was triggered due to queries change , reset the flag
          manager.wsQueriesChange = false;
        }
      };
      // Purposefully ignoring 'error' events; they are always followed by a 'close' event, which needs to be handled anyway

      ws.addEventListener("open", onWsOpen);
      ws.addEventListener("message", onWsMessage(ws));
      ws.addEventListener("close", onWsClose);

      //eslint-disable-next-line @typescript-eslint/no-this-alias
      manager.ws = ws;
    }
  }

  constructor({
    token,
    wsBaseUrl,
    reconnectThreshold,
  }: {
    token: string;
    wsBaseUrl: string;
    reconnectThreshold: number;
  }) {
    this.protocols = ["jwt.token." + token, "daml.ws.auth"];
    this.url = wsBaseUrl + QueryStreamsManager.ENDPOINT;
    this.reconnectThresholdMs = reconnectThreshold;
  }

  streamSubmit<T extends object, K, I extends string>(
    template: TemplateOrInterface<T, K, I>,
    queries: Query<T>[],
    caller: string,
  ): Stream<T, K, I, readonly CreateEvent<T, K, I>[]> {
    //eslint-disable-next-line @typescript-eslint/no-this-alias
    const manager = this;
    const query: StreamingQuery<T, K, I> = {
      template,
      queries,
      stream: new StreamEventEmitter({
        beforeClosing(): void {
          manager.queries.delete(
            query as unknown as StreamingQuery<object, unknown, string>,
          );
          // if no more queries then just let it go
          if (manager.queries.size > 0) {
            manager.handleQueriesChange();
          }
        },
      }),
      state: new Map(),
      offset: NoOffsetReceivedYet,
      caller,
    };
    manager.queries.add(
      query as unknown as StreamingQuery<object, unknown, string>,
    );
    manager.handleQueriesChange();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const on = (type: string, listener: any): void => {
      if (
        manager.ws?.readyState === WsState.Open ||
        manager.ws?.readyState === WsState.Connecting
      ) {
        query.stream.on(type, listener);
      } else {
        console.error("Trying to add a listener to a closed stream.");
      }
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const off = (type: string, listener: any): void => {
      if (
        manager.ws?.readyState === WsState.Open ||
        manager.ws?.readyState === WsState.Connecting
      ) {
        query.stream.off(type, listener);
      } else {
        console.error("Trying to remove a listener from a closed stream.");
      }
    };
    const close = (): void => {
      query.stream.close();
    };
    return { on, off, close };
  }
}

/**
 * An object of type `Ledger` represents a handle to a Daml ledger.
 */
class Ledger {
  private readonly token: string;
  private readonly httpBaseUrl: string;
  private readonly wsBaseUrl: string;
  private readonly reconnectThreshold: number;
  private readonly multiplexQueryStreams: boolean;
  private readonly queryStreamsManager: QueryStreamsManager;

  /**
   * Construct a new `Ledger` object. See [[LedgerOptions]] for the constructor arguments.
   */
  constructor({
    token,
    httpBaseUrl,
    wsBaseUrl,
    reconnectThreshold = 30000,
    multiplexQueryStreams = false,
  }: LedgerOptions) {
    if (!httpBaseUrl) {
      httpBaseUrl = `${window.location.protocol}//${window.location.host}/`;
    }
    if (
      !(httpBaseUrl.startsWith("http://") || httpBaseUrl.startsWith("https://"))
    ) {
      throw Error(
        `Ledger: httpBaseUrl must start with 'http://' or 'https://'. (${httpBaseUrl})`,
      );
    }
    if (!httpBaseUrl.endsWith("/")) {
      throw Error(`Ledger: httpBaseUrl must end with '/'. (${httpBaseUrl})`);
    }
    if (!wsBaseUrl) {
      wsBaseUrl = "ws" + httpBaseUrl.slice(4);
    }
    if (!(wsBaseUrl.startsWith("ws://") || wsBaseUrl.startsWith("wss://"))) {
      throw Error(
        `Ledger: wsBaseUrl must start with 'ws://' or 'wss://'. (${wsBaseUrl})`,
      );
    }
    if (!wsBaseUrl.endsWith("/")) {
      throw Error(`Ledger: wsBaseUrl must end with '/'. (${wsBaseUrl})`);
    }

    this.token = token;
    this.httpBaseUrl = httpBaseUrl;
    this.wsBaseUrl = wsBaseUrl;
    this.reconnectThreshold = reconnectThreshold;
    this.multiplexQueryStreams = multiplexQueryStreams;
    this.queryStreamsManager = new QueryStreamsManager({
      token,
      wsBaseUrl,
      reconnectThreshold,
    });
  }

  /**
   * @internal
   */
  private auth(): { [headers: string]: string } {
    return { Authorization: "Bearer " + this.token };
  }

  /**
   * @internal
   */
  private async throwOnError(r: Response): Promise<void> {
    if (!r.ok) {
      const json = await r.json();
      console.log(json);
      throw decode(decodeLedgerError, json);
    }
  }

  /**
   * @internal
   *
   * Internal function to submit a command to the JSON API.
   */
  private async submit(
    endpoint: string,
    payload: unknown,
    method = "post",
  ): Promise<unknown> {
    const httpResponse = await fetch(this.httpBaseUrl + endpoint, {
      body: JSON.stringify(payload),
      headers: {
        ...this.auth(),
        "Content-type": "application/json",
      },
      method,
    });
    await this.throwOnError(httpResponse);
    const json = await httpResponse.json();
    const ledgerResponse = jtv.Result.withException(
      decodeLedgerResponse.run(json),
    );
    if (ledgerResponse.warnings) {
      console.warn(ledgerResponse.warnings);
    }
    return ledgerResponse.result;
  }

  /**
   * Retrieve contracts for a given template.
   *
   * When no `query` argument is given, all contracts visible to the submitting party are returned.
   * When a `query` argument is given, only those contracts matching the query are returned. See
   * https://docs.daml.com/json-api/search-query-language.html for a description of the query
   * language.
   *
   * @param template The contract template of the contracts to be matched against.
   * @param query The contract query for the contracts to be matched against.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  async query<T extends object, K, I extends string>(
    template: TemplateOrInterface<T, K, I>,
    query?: Query<T>,
  ): Promise<CreateEvent<T, K, I>[]> {
    const payload = {
      templateIds: [template.templateId],
      query: encodeQuery(template, query),
    };
    const json = await this.submit("v1/query", payload);
    return jtv.Result.withException(
      jtv.array(decodeCreateEvent(template)).run(json),
    );
  }

  /**
   * Fetch a contract identified by its contract ID.
   *
   * @param template The template of the contract to be fetched.
   * @param contractId The contract id of the contract to be fetched.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  async fetch<T extends object, K, I extends string>(
    template: TemplateOrInterface<T, K, I>,
    contractId: ContractId<T>,
  ): Promise<CreateEvent<T, K, I> | null> {
    const payload = {
      templateId: template.templateId,
      contractId: ContractId(template).encode(contractId),
    };
    const json = await this.submit("v1/fetch", payload);
    return jtv.Result.withException(
      jtv.oneOf(jtv.constant(null), decodeCreateEvent(template)).run(json),
    );
  }

  /**
   * Fetch a contract identified by its contract key.
   *
   * Same as [[fetch]], but the contract to be fetched is identified by its contract key instead of
   * its contract id.
   *
   * @param template The template of the contract to be fetched.
   * @param key The contract key of the contract to be fetched.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   */
  async fetchByKey<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    key: K,
  ): Promise<CreateEvent<T, K, I> | null> {
    if (key === undefined) {
      throw Error(
        `Cannot lookup by key on template ${template.templateId} because it does not define a key.`,
      );
    }
    const payload = {
      templateId: template.templateId,
      key: template.keyEncode(key),
    };
    const json = await this.submit("v1/fetch", payload);
    return jtv.Result.withException(
      jtv.oneOf(jtv.constant(null), decodeCreateEvent(template)).run(json),
    );
  }

  /**
   * Create a contract for a given template.
   *
   * @param template The template of the contract to be created.
   * @param payload The template arguments for the contract to be created.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  async create<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    payload: T,
  ): Promise<CreateEvent<T, K, I>> {
    const command = {
      templateId: template.templateId,
      payload: template.encode(payload),
    };
    const json = await this.submit("v1/create", command);
    return jtv.Result.withException(decodeCreateEvent(template).run(json));
  }

  /**
   * Exercise a choice on a contract identified by its contract ID.
   *
   * @param choice The choice to exercise.
   * @param contractId The contract id of the contract to exercise.
   * @param argument The choice arguments.
   *
   * @typeparam T The contract template type.
   * @typeparam C The type of the contract choice.
   * @typeparam R The return type of the choice.
   *
   * @returns The return value of the choice together with a list of
   * [[event]]'s that were created as a result of exercising the choice.
   */
  async exercise<T extends object, C, R, K>(
    choice: Choice<T, C, R, K>,
    contractId: ContractId<T>,
    argument: C,
  ): Promise<[R, Event<object>[]]> {
    const payload = {
      templateId: choice.template().templateId,
      contractId: ContractId(choice.template()).encode(contractId),
      choice: choice.choiceName,
      argument: choice.argumentEncode(argument),
    };
    const json = await this.submit("v1/exercise", payload);
    // Decode the server response into a tuple.
    const responseDecoder: jtv.Decoder<{
      exerciseResult: R;
      events: Event<object>[];
    }> = jtv.object({
      exerciseResult: choice.resultDecoder,
      events: jtv.array(decodeEventUnknown),
    });
    const { exerciseResult, events } = jtv.Result.withException(
      responseDecoder.run(json),
    );
    return [exerciseResult, events];
  }

  /**
   * Exercse a choice on a newly-created contract, in a single transaction.
   *
   * @param choice The choice to exercise.
   * @param payload The template arguments for the newly-created contract.
   * @param argument The choice arguments.
   *
   * @typeparam T The contract template type.
   * @typeparam C The type of the contract choice.
   * @typeparam R The return type of the choice.
   *
   * @returns The return value of the choice together with a list of
   * [[event]]'s that includes the creation event for the created contract as
   * well as all the events that were created as a result of exercising the
   * choice, including the archive event for the created contract if the choice
   * is consuming (or otherwise archives it as part of its execution).
   *
   */
  async createAndExercise<T extends object, C, R, K>(
    choice: ChoiceFrom<Template<T, K>> & Choice<T, C, R, K>,
    payload: T,
    argument: C,
  ): Promise<[R, Event<object>[]]> {
    const command = {
      templateId: choice.template().templateId,
      payload: choice.template().encode(payload),
      choice: choice.choiceName,
      argument: choice.argumentEncode(argument),
    };
    const json = await this.submit("v1/create-and-exercise", command);

    const responseDecoder: jtv.Decoder<{
      exerciseResult: R;
      events: Event<object>[];
    }> = jtv.object({
      exerciseResult: choice.resultDecoder,
      events: jtv.array(decodeEventUnknown),
    });
    const { exerciseResult, events } = jtv.Result.withException(
      responseDecoder.run(json),
    );
    return [exerciseResult, events];
  }

  /**
   * Exercise a choice on a contract identified by its contract key.
   *
   * Same as [[exercise]], but the contract is identified by its contract key instead of its
   * contract id.
   *
   * @param choice The choice to exercise.
   * @param key The contract key of the contract to exercise.
   * @param argument The choice arguments.
   *
   * @typeparam T The contract template type.
   * @typeparam C The type of the contract choice.
   * @typeparam R The return type of the choice.
   * @typeparam K The type of the contract key.
   *
   * @returns The return value of the choice together with a list of [[event]]'s that where created
   * as a result of exercising the choice.
   */
  async exerciseByKey<T extends object, C, R, K>(
    choice: ChoiceFrom<Template<T, K>> & Choice<T, C, R, K>,
    key: K,
    argument: C,
  ): Promise<[R, Event<object>[]]> {
    if (key === undefined) {
      throw Error(
        `Cannot exercise by key on template ${
          choice.template().templateId
        } because it does not define a key.`,
      );
    }
    const payload = {
      templateId: choice.template().templateId,
      key: choice.template().keyEncode(key),
      choice: choice.choiceName,
      argument: choice.argumentEncode(argument),
    };
    const json = await this.submit("v1/exercise", payload);
    // Decode the server response into a tuple.
    const responseDecoder: jtv.Decoder<{
      exerciseResult: R;
      events: Event<object>[];
    }> = jtv.object({
      exerciseResult: choice.resultDecoder,
      events: jtv.array(decodeEventUnknown),
    });
    const { exerciseResult, events } = jtv.Result.withException(
      responseDecoder.run(json),
    );
    return [exerciseResult, events];
  }

  /**
   * Archive a contract identified by its contract ID.
   *
   * @param template The template of the contract to archive.
   * @param contractId The contract id of the contract to archive.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  async archive<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    contractId: ContractId<T>,
  ): Promise<ArchiveEvent<T, I>> {
    return decodeArchiveResponse(template, "archive", () =>
      this.exercise(template.Archive, contractId, {}),
    );
  }

  /**
   * Archive a contract identified by its contract key.
   * Same as [[archive]], but the contract to be archived is identified by its contract key.
   *
   * @param template The template of the contract to be archived.
   * @param key The contract key of the contract to be archived.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  async archiveByKey<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    key: K,
  ): Promise<ArchiveEvent<T, I>> {
    return decodeArchiveResponse(template, "archiveByKey", () =>
      this.exerciseByKey(template.Archive, key, {}),
    );
  }

  /**
   * @internal
   *
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
    callerName: string,
    template: TemplateOrInterface<T, K, I>,
    endpoint: string,
    request: unknown,
    reconnectRequest: () => unknown,
    init: State,
    change: (state: State, events: readonly Event<T, K, I>[]) => State,
  ): Stream<T, K, I, State> {
    const protocols = ["jwt.token." + this.token, "daml.ws.auth"];
    let ws = new WebSocket(this.wsBaseUrl + endpoint, protocols);
    let isLiveSince: undefined | number = undefined;
    let lastOffset: undefined | null | string = undefined;
    let state = init;
    let isReconnecting: boolean = false;
    let streamClosed: boolean = false;
    const emitter = new EventEmitter();
    const onWsOpen = (): void => {
      if (isReconnecting) {
        // the JSON API server can't handle null offsets, even though it sends them out under
        // special conditions when there are no transactions yet. Not sending the `offset` message
        // will start the stream from the very beginning of the transaction log.
        if (lastOffset !== null)
          ws.send(JSON.stringify({ offset: lastOffset }));
        ws.send(JSON.stringify(reconnectRequest()));
      } else {
        ws.send(JSON.stringify(request));
      }
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const onWsMessage = (event: { data: any }): void => {
      const json: unknown = JSON.parse(event.data.toString());
      if (isRecordWith("events", json)) {
        const events = jtv.Result.withException(
          jtv.array(decodeEvent(template)).run(json.events),
        );
        if (events.length > 0) {
          state = change(state, events);
          emitter.emit("change", state, events);
        }
        if (isRecordWith("offset", json)) {
          lastOffset = jtv.Result.withException(
            jtv.oneOf(jtv.constant(null), jtv.string()).run(json.offset),
          );
          if (isLiveSince === undefined) {
            isLiveSince = Date.now();
            emitter.emit("live", state);
          }
        }
      } else if (isRecordWith("warnings", json)) {
        console.warn(`${callerName} warnings`, json);
      } else if (isRecordWith("errors", json)) {
        console.error(`${callerName} errors`, json);
      } else {
        console.error(`${callerName} unknown message`, json);
      }
    };
    const closeStream = (status: { code: number; reason: string }): void => {
      streamClosed = true;
      emitter.emit("close", status);
      emitter.removeAllListeners();
    };
    const onWsClose = (): void => {
      if (streamClosed === false) {
        const now = new Date().getTime();
        // we want to try and keep the stream open, so we try to reconnect
        // the underlying ws
        if (
          lastOffset !== undefined &&
          isLiveSince !== undefined &&
          now - isLiveSince >= this.reconnectThreshold
        ) {
          isLiveSince = undefined;
          isReconnecting = true;
          ws = new WebSocket(this.wsBaseUrl + endpoint, protocols);
          ws.addEventListener("open", onWsOpen);
          ws.addEventListener("message", onWsMessage);
          ws.addEventListener("close", onWsClose);
        } else {
          // ws has closed too quickly / never managed to connect: we give up
          closeStream({ code: 4001, reason: "ws connection failed" });
        }
      } // no else: if the stream is closed we don't need to keep a ws
    };
    ws.addEventListener("open", onWsOpen);
    ws.addEventListener("message", onWsMessage);
    // NOTE(MH): We ignore the 'error' event since it is always followed by a
    // 'close' event, which we need to handle anyway.
    ws.addEventListener("close", onWsClose);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const on = (type: string, listener: any): void => {
      if (streamClosed === false) {
        void emitter.on(type, listener);
      } else {
        console.error("Trying to add a listener to a closed stream.");
      }
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const off = (type: string, listener: any): void => {
      if (streamClosed === false) {
        void emitter.off(type, listener);
      } else {
        console.error("Trying to remove a listener from a closed stream.");
      }
    };
    const close = (): void => {
      // Note: ws.close will trigger the onClose handlers of the WebSocket
      // (here onWsClose), but they execute as a separate event after the
      // current event in the JS event loop, i.e. in particular after the call
      // to closeStream and thus, in this case, the onWsClose handler will see
      // streamClosed as true.
      ws.close();
      closeStream({ code: 4000, reason: "called .close()" });
    };
    return { on, off, close };
  }

  /**
   * Retrieve a consolidated stream of events for a given template and query.
   *
   * The accumulated state is the current set of active contracts matching the query. When no
   * `query` argument is given, all events visible to the submitting party are returned. When a
   * `query` argument is given, only those create events matching the query are returned. See
   * https://docs.daml.com/json-api/search-query-language.html for a description of the query
   * language.
   *
   * @deprecated Prefer `streamQueries`.
   *
   * @param template The contract template to match contracts against.
   * @param query The query to match contracts agains.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  streamQuery<T extends object, K, I extends string>(
    template: TemplateOrInterface<T, K, I>,
    query?: Query<T>,
  ): Stream<T, K, I, readonly CreateEvent<T, K, I>[]> {
    if (query === undefined) {
      return this.streamQueryCommon(template, [], "Ledger.streamQuery");
    } else {
      return this.streamQueryCommon(template, [query], "Ledger.streamQuery");
    }
  }

  /**
   * @internal
   *
   */
  private streamQueryCommon<T extends object, K, I extends string>(
    template: TemplateOrInterface<T, K, I>,
    queries: Query<T>[],
    name: string,
  ): Stream<T, K, I, readonly CreateEvent<T, K, I>[]> {
    if (this.multiplexQueryStreams) {
      return this.queryStreamsManager.streamSubmit(template, queries, name);
    } else {
      const request =
        queries.length == 0
          ? [{ templateIds: [template.templateId] }]
          : queries.map(q => ({
              templateIds: [template.templateId],
              query: encodeQuery(template, q),
            }));
      const reconnectRequest = (): object[] => request;
      const change = (
        contracts: readonly CreateEvent<T, K, I>[],
        events: readonly Event<T, K, I>[],
      ): CreateEvent<T, K, I>[] => {
        const archiveEvents: Set<ContractId<T>> = new Set();
        const createEvents: CreateEvent<T, K, I>[] = [];
        for (const event of events) {
          if ("created" in event) {
            createEvents.push(event.created);
          } else if ("archived" in event) {
            archiveEvents.add(event.archived.contractId);
          }
        }
        return contracts
          .concat(createEvents)
          .filter(contract => !archiveEvents.has(contract.contractId));
      };
      return this.streamSubmit(
        name,
        template,
        "v1/stream/query",
        request,
        reconnectRequest,
        [],
        change,
      );
    }
  }

  /**
   * Retrieve a consolidated stream of events for a given template and queries.
   *
   * If the given list is empty, the accumulated state is the set of all active
   * contracts for the given template. Otherwise, the accumulated state is the
   * set of all contracts that match at least one of the given queries.
   *
   * See https://docs.daml.com/json-api/search-query-language.html for a
   * description of the query language.
   *
   * @param template The contract template to match contracts against.
   * @param queries A query to match contracts against.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   */
  streamQueries<T extends object, K, I extends string>(
    template: TemplateOrInterface<T, K, I>,
    queries: Query<T>[],
  ): Stream<T, K, I, readonly CreateEvent<T, K, I>[]> {
    return this.streamQueryCommon(template, queries, "Ledger.streamQueries");
  }

  /**
   * Retrieve a consolidated stream of events for a given template and contract key.
   *
   * The accumulated state is either the current active contract for the given
   * key, or null if there is no active contract for the given key.
   *
   * @deprecated Prefer `streamFetchByKeys`.
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   *
   */
  streamFetchByKey<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    key: K,
  ): Stream<T, K, I, CreateEvent<T, K, I> | null> {
    // Note: this implementation is deliberately not unified with that of
    // `streamFetchByKeys`, because doing so would add the requirement that the
    // given key be in output format, whereas existing implementation supports
    // input format.
    let lastContractId: ContractId<T> | null = null;
    const request = [
      { templateId: template.templateId, key: template.keyEncode(key) },
    ];
    const reconnectRequest = (): object[] => [
      {
        ...request[0],
        contractIdAtOffset:
          lastContractId && ContractId(template).encode(lastContractId),
      },
    ];
    const change = (
      contract: CreateEvent<T, K, I> | null,
      events: readonly Event<T, K, I>[],
    ): CreateEvent<T, K, I> | null => {
      for (const event of events) {
        if ("created" in event) {
          contract = event.created;
        } else {
          // i.e. 'archived' event
          if (contract && contract.contractId === event.archived.contractId) {
            contract = null;
          }
        }
      }
      lastContractId = contract ? contract.contractId : null;
      return contract;
    };
    return this.streamSubmit(
      "Ledger.streamFetchByKey",
      template,
      "v1/stream/fetch",
      request,
      reconnectRequest,
      null,
      change,
    );
  }

  /**
   * @internal
   *
   * Returns the same API as [[streamSubmit]] but does not, in fact, establish
   * any socket connection. Instead, this is a stream that always has the given
   * value as its accumulated state.
   */
  private constantStream<T extends object, K, I extends string, V>(
    value: V,
  ): Stream<T, K, I, V> {
    function on(t: "live", l: (v: V) => void): void;
    function on(
      t: "change",
      l: (v: V, events: readonly Event<T, K, I>[]) => void,
    ): void;
    function on(t: "close", l: (e: StreamCloseEvent) => void): void;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    function on(type: any, listener: any): any {
      if (type === "live") {
        listener(value);
      }
      if (type === "change") {
        listener(value, []);
      }
    }
    function off(t: "live", l: (v: V) => void): void;
    function off(
      t: "change",
      l: (v: V, events: readonly Event<T, K, I>[]) => void,
    ): void;
    function off(t: "close", l: (e: StreamCloseEvent) => void): void;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars, @typescript-eslint/no-empty-function
    function off(_t: any, _l: any): any {}
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    return { on, off, close: (): void => {} };
  }

  /**
   * Retrieve a consolidated stream of events for a list of keys and a single
   * template.
   *
   * The accumulated state is an array of the same length as the given list of
   * keys, with positional correspondence. Each element in the array represents
   * the current contract for the given key, or is explicitly null if there is
   * currently no active contract matching that key.
   *
   * Note: the given `key` objects will be compared for (deep) equality with
   * the values returned by the API. As such, they have to be given in the
   * "output" format of the API, including the values of
   * `encodeDecimalAsString` and `encodeInt64AsString`. See [the JSON API docs
   * for details](https://docs.daml.com/json-api/lf-value-specification.html).
   *
   * @typeparam T The contract template type.
   * @typeparam K The contract key type.
   * @typeparam I The contract id type.
   */
  streamFetchByKeys<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keys: K[],
  ): Stream<T, K, I, (CreateEvent<T, K, I> | null)[]> {
    // We support zero-length key so clients can more easily manage a dynamic
    // list, without having to special-case 0-length on their side.
    if (keys.length == 0) {
      return this.constantStream([]);
    }
    const lastContractIds: (ContractId<T> | null)[] = Array(keys.length).fill(
      null,
    );
    const keysCopy = _.cloneDeep(keys);
    const initState: (CreateEvent<T, K, I> | null)[] = Array(keys.length).fill(
      null,
    );
    const request = keys.map(k => ({
      templateId: template.templateId,
      key: template.keyEncode(k),
    }));
    const reconnectRequest = (): object[] =>
      request.map((r, idx) => {
        const lastId = lastContractIds[idx];
        return {
          ...r,
          contractIdAtOffset: lastId && ContractId(template).encode(lastId),
        };
      });
    const change = (
      state: (CreateEvent<T, K, I> | null)[],
      events: readonly Event<T, K, I>[],
    ): (CreateEvent<T, K, I> | null)[] => {
      const newState: (CreateEvent<T, K, I> | null)[] = Array.from(state);
      for (const event of events) {
        if ("created" in event) {
          const k = event.created.key;
          keysCopy.forEach((requestKey, idx) => {
            if (_.isEqual(requestKey, k)) {
              newState[idx] = event.created;
            }
          });
        } else {
          // i.e. 'archived' in event
          const id: ContractId<T> = event.archived.contractId;
          newState.forEach((contract, idx) => {
            if (contract && contract.contractId === id) {
              newState[idx] = null;
            }
          });
        }
      }
      newState.forEach((c, idx) => {
        lastContractIds[idx] = c ? c.contractId : null;
      });
      return newState;
    };
    return this.streamSubmit(
      "streamFetchByKeys",
      template,
      "v1/stream/fetch",
      request,
      reconnectRequest,
      initState,
      change,
    );
  }

  /**
   * Fetch parties by identifier.
   *
   * @param parties An array of Party identifiers.
   *
   * @returns An array of the same length, where each element corresponds to
   * the same-index element of the given parties, ans is either a PartyInfo
   * object if the party exists or null if it does not.
   *
   */
  async getParties(parties: Party[]): Promise<(PartyInfo | null)[]> {
    if (parties.length === 0) {
      return [];
    }
    const json = await this.submit("v1/parties", parties);
    const resp: PartyInfo[] = decode(jtv.array(partyInfoDecoder), json);
    const mapping: { [ps: string]: PartyInfo } = {};
    for (const p of resp) {
      mapping[p.identifier] = p;
    }
    const ret: (PartyInfo | null)[] = Array(parties.length).fill(null);
    for (let idx = 0; idx < parties.length; idx++) {
      ret[idx] = mapping[parties[idx]] || null;
    }
    return ret;
  }

  /**
   * Fetch all parties on the ledger.
   *
   * @returns All parties on the ledger, in no particular order.
   *
   */
  async listKnownParties(): Promise<PartyInfo[]> {
    const json = await this.submit("v1/parties", undefined, "get");
    return decode(jtv.array(partyInfoDecoder), json);
  }

  /**
   * Get the current user details obtained by the currently used JWT.
   *
   * @param userId The user id
   *
   * @returns User details
   *
   */
  async getUser(userId?: string): Promise<User> {
    const json = isUndefined(userId)
      ? await this.submit("v1/user", undefined, "get")
      : await this.submit("v1/user", { userId: userId }, "post");
    return decode(userDecoder, json);
  }

  /**
   * Lists the users on the ledger
   *
   * @returns user list
   *
   */
  async listUsers(): Promise<User[]> {
    const json = await this.submit("v1/users", undefined, "get");
    return decode(jtv.array(userDecoder), json);
  }

  /**
   * Lists the rights associated with the given user id
   *
   * @param userId, if empty then the user id will obtained by the currently used JWT.
   *
   * @returns list of user rights
   */
  async listUserRights(userId?: string): Promise<UserRight[]> {
    const json = isUndefined(userId)
      ? await this.submit("v1/user/rights", undefined, "get")
      : await this.submit("v1/user/rights", { userId: userId });
    return decode(jtv.array(userRightDecoder), json);
  }

  /**
   * Grants rights to a user
   *
   * @param userId The user to which rights shall be granted
   *
   * @param rights The rights which shall be granted
   *
   * @returns The rights which actually were granted (if a right was already granted, then it will not be in the return list)
   */
  async grantUserRights(
    userId: string,
    rights: UserRight[],
  ): Promise<UserRight[]> {
    const json = await this.submit("v1/user/rights/grant", {
      userId: userId,
      rights: rights,
    });
    return decode(jtv.array(userRightDecoder), json);
  }

  /**
   * Revokes rights from a user
   *
   * @param userId The user from which rights shall be revoked
   *
   * @param rights The rights which shall be revoked
   *
   * @returns The rights which actually were revoked (if a right was already revoked, then it will not be in the return list)
   */
  async revokeUserRights(
    userId: string,
    rights: UserRight[],
  ): Promise<UserRight[]> {
    const json = await this.submit("v1/user/rights/revoke", {
      userId: userId,
      rights: rights,
    });
    return decode(jtv.array(userRightDecoder), json);
  }

  /**
   * Creates a user
   *
   * @param userId The user ID
   * @param rights The initial rights the user should have
   * @param primaryParty The primary party the user should have
   *
   */
  async createUser(
    userId: string,
    rights: UserRight[],
    primaryParty?: string,
  ): Promise<void> {
    await this.submit("v1/user/create", {
      userId: userId,
      rights: rights,
      primaryParty: primaryParty,
    });
  }

  /**
   * Deletes a user
   *
   * @param userId The user ID
   * @param rights The initial rights the user should have
   * @param primaryParty The primary party the user should have
   *
   */
  async deleteUser(userId: string): Promise<void> {
    await this.submit("v1/user/delete", { userId: userId });
  }

  /**
   * Allocate a new party.
   *
   * @param partyOpt Parameters for party allocation.
   *
   * @returns PartyInfo for the newly created party.
   *
   */
  async allocateParty(partyOpt: {
    identifierHint?: string;
    displayName?: string;
  }): Promise<PartyInfo> {
    const json = await this.submit("v1/parties/allocate", partyOpt);
    return decode(partyInfoDecoder, json);
  }

  /**
   * Fetch a list of all package IDs from the ledger.
   *
   * @returns List of package IDs.
   *
   */
  async listPackages(): Promise<PackageId[]> {
    const json = await this.submit("v1/packages", undefined, "get");
    return decode(jtv.array(jtv.string()), json);
  }

  /**
   * Fetch a binary package.
   *
   * @returns The content of the package as a raw ArrayBuffer.
   *
   */
  async getPackage(id: PackageId): Promise<ArrayBuffer> {
    const httpResponse = await fetch(this.httpBaseUrl + "v1/packages/" + id, {
      headers: this.auth(),
      method: "get",
    });
    await this.throwOnError(httpResponse);
    return await httpResponse.arrayBuffer();
  }

  /**
   * Upload a binary archive. Note that this requires admin privileges.
   *
   * @returns No return value on success; throws on error.
   *
   */
  async uploadDarFile(abuf: ArrayBuffer): Promise<void> {
    const httpResponse = await fetch(this.httpBaseUrl + "v1/packages", {
      body: abuf,
      headers: {
        ...this.auth(),
        "Content-type": "application/octet-stream",
      },
      method: "post",
    });
    await this.throwOnError(httpResponse);
    return;
  }
}

export default Ledger;
