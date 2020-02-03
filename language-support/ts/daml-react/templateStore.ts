// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as immutable from 'immutable';
import { Event, Query, CreateEvent} from '@daml/ledger';
import { ContractId } from '@daml/types';
import deepEqual from 'fast-deep-equal';

export type QueryResult<T extends object, K> = {
  contracts: CreateEvent<T, K>[];
  loading: boolean;
}

export type FetchResult<T extends object, K> = {
  contract: CreateEvent<T, K> | null;
  loading: boolean;
}

export type Store<T extends object, K> = {
  queryResults: immutable.Map<Query<T>, QueryResult<T, K>>;
  fetchByKeyResults: immutable.Map<K, FetchResult<T, K>>;
}

export const emptyQueryResult = <T extends object, K>(): QueryResult<T, K> => ({
  contracts: [],
  loading: false,
});

export const emptyFetchResult = <T extends object, K>(): FetchResult<T, K> => ({
  contract: null,
  loading: false,
});

export const empty = <T extends object, K>(): Store<T, K> => ({
  queryResults: immutable.Map(),
  fetchByKeyResults: immutable.Map(),
});

export const setAllLoading = <T extends object, K>(store: Store<T, K>): Store<T, K> => ({
  queryResults: store.queryResults.map((res) => ({...res, loading: true})),
  fetchByKeyResults: store.fetchByKeyResults.map((res) => ({...res, laoding: true})),
});

export const setQueryLoading = <T extends object, K>(store: Store<T, K>, query: Query<T>): Store<T, K> => ({
  ...store,
  queryResults: store.queryResults.update(query, (res = emptyQueryResult()) => ({...res, loading: true})),
});

export const setQueryResult = <T extends object, K>(store: Store<T, K>, query: Query<T>, contracts: CreateEvent<T, K>[]): Store<T, K> => ({
  ...store,
  queryResults: store.queryResults.set(query, {contracts, loading: false})
});

export const payloadMatchesQuery = <T>(payload: T, query: Query<T>): boolean => {
  if (typeof payload === 'object' && typeof query === 'object') {
    const keys = Object.keys(query) as (keyof T & keyof Query<T>)[]
    return keys.reduce<boolean>(
      // TODO(MH): We should consider some form of crashing/logging when the
      // `key` below is not a key of `payload`.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (acc, key) => acc && key in payload && payloadMatchesQuery(payload[key], query[key] as any),
      true,
    );
  } else {
    return typeof payload === typeof query && payload === query
  }
}


export const addEvents = <T extends object, K>(store: Store<T, K>, events: Event<T, K>[]): Store<T, K> => {
  const archived: Set<ContractId<T>> = new Set();
  const created: CreateEvent<T, K>[] = [];
  for (const event of events) {
    if ('created' in event) {
      const createEvent = event.created;
      if (archived.has(createEvent.contractId)) {
        // NOTE(MH): The handling of contact keys below would do the wrong
        // thing when the archive event for a contract comes before its create
        // event. Since the JSON API is not supposed to produce event series
        // with this property, we fail loudly should it ever do so.
        throw Error(`TemplateStore.addEvent: bad order of create and archive events for \
          contract ID ${createEvent.contractId} for template ${createEvent.templateId}.`);
      } else {
        created.push(createEvent);
      }
    } else { // i.e. 'archived' in event
      archived.add(event.archived.contractId);
    }
  }
  const queryResults = store.queryResults.map((queryResult, query) => {
    const contracts = queryResult.contracts
      .concat(created.filter((event) => payloadMatchesQuery(event.payload, query)))
      .filter((contract) => !archived.has(contract.contractId));
    return {...queryResult, contracts};
  });

  // NOTE(MH): Since `immutable.Map` uses reference equality for keys when they
  // are regular JavaScript objects, we cannot simply walk over all create
  // events and use their key as an index into `store.fetchByKeyResults` but
  // rather have to walk over the whole cross product of
  // `store.fetchByKeyResults` and `events`.
  const fetchByKeyResults = store.fetchByKeyResults.map((result, key) => {
    let contract = result.contract;
    for (const event of events) {
      if ('created' in event) {
        const createEvent = event.created;
        if (deepEqual(key, createEvent.key)) {
          // TODO(MH, #4360): If we were sure we never miss any events and
          // always get them in causality order, `contract` would always be
          // `null` when we get here and we should throw an exception
          // otherwise. However, the current impementation does not even
          // attempt to get all events...
          contract = createEvent;
        }
      } else { // i.e. 'archived' in event
        if (contract !== null && contract.contractId === event.archived.contractId) {
          contract = null;
        }
      }
    }
    return {...result, contract};
  });

  return {queryResults, fetchByKeyResults};
}

export const setFetchByKeyLoading = <T extends object, K>(store: Store<T, K>, key: K): Store<T, K> => ({
  ...store,
  fetchByKeyResults: store.fetchByKeyResults.update(key, (res = emptyFetchResult()) => ({...res, loading: true})),
});

export const setFetchByKeyResult = <T extends object, K>(store: Store<T, K>, key: K, contract: CreateEvent<T, K> | null) => ({
  ...store,
  fetchByKeyResults: store.fetchByKeyResults.set(key, {contract, loading: false}),
});
