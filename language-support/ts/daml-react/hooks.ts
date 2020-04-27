// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Template, Party } from "@daml/types";
import Ledger, { CreateEvent, Query } from '@daml/ledger';
import { useState, useContext, useEffect } from "react";
import { DamlLedgerState, DamlLedgerContext } from './context'

// NOTE(MH, useEffect dependencies): There are various places in this file
// where we need to maintain the dependencies of the `useEffect` hook manually
// and there's no tool to help us enfore they are correct. Thus, we need to be
// extra careful in these locations. If we add too many dependencies, we will
// make unnecessary network requests. If we forget adding some dependencies, we
// not make a new network request although they are required to refresh data.


/**
 * @internal
 */
const useDamlState = (): DamlLedgerState => {
  const state = useContext(DamlLedgerContext);
  if (!state) {
    throw Error("Trying to use DamlLedgerContext before initializing.")
  }
  return state;
}

/**
 * React hook to get the party currently connected to the ledger.
 */
export const useParty = (): Party => {
  const state = useDamlState();
  return state.party;
}

/**
 * React Hook that returns the Ledger instance to interact with the connected DAML ledger.
 */
export const useLedger = (): Ledger => {
  return useDamlState().ledger;
}

/**
 * The result of a query against the ledger.
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
}

/**
 * React Hook for a ``query`` against the ledger.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The contract template to filter for.
 * @param queryFactory A function returning a query. If the query is omitted, all visible contracts of the given template are returned.
 * @param queryDeps The dependencies of the query (which trigger a reload when changed).
 *
 * @return The result of the query.
 */
export function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory: () => Query<T>, queryDeps: readonly unknown[]): QueryResult<T, K, I>
export function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>): QueryResult<T, K, I>
export function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]): QueryResult<T, K, I> {
  const state = useDamlState();
  const [result, setResult] = useState<QueryResult<T, K, I>>({contracts: [], loading: false});
  useEffect(() => {
    setResult({contracts: [], loading: true});
    const query = queryFactory ? queryFactory() : undefined;
    const load = async (): Promise<void> => {
      const contracts = await state.ledger.query(template, query);
      setResult({contracts, loading: false});
    };
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    load();
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, state.reloadToken, template, ...(queryDeps ?? [])]);
  return result;
}

/**
 * The result of a ``fetch`` against the ledger.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 */
export type FetchResult<T extends object, K, I extends string> = {
  /** Contracts of the given contract template and key. */
  contract: CreateEvent<T, K, I> | null;
  /** Indicator for whether the fetch is executing. */
  loading: boolean;
}

/**
 * React Hook for a lookup by key against the `/v1/fetch` endpoint of the JSON API.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to fetch.
 * @param keyFactory A function returning the contract key of the contracts to fetch.
 * @param keyDeps Dependencies of this hook (for which the fetch is reexecuted on change).
 *
 * @return The fetched contract.
 */
export function useFetchByKey<T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]): FetchResult<T, K, I> {
  const state = useDamlState();
  const [result, setResult] = useState<FetchResult<T, K, I>>({contract: null, loading: false});
  useEffect(() => {
    const key = keyFactory();
    setResult({contract: null, loading: true});
    const load = async (): Promise<void> => {
      const contract = await state.ledger.fetchByKey(template, key);
      setResult({contract, loading: false});
    };
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    load();
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, state.reloadToken, template, ...(keyDeps ?? [])]);
  return result;
}

/**
 * React Hook to query the ledger, the returned result is updated as the ledger state changes.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to match.
 * @param queryFactory A function returning a query. If the query is omitted, all visible contracts of the given template are returned.
 * @param queryDeps The dependencies of the query (for which a change triggers an update of the result)
 *
 * @return The matching contracts.
 *
 */
export function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory: () => Query<T>, queryDeps: readonly unknown[]): QueryResult<T, K, I>
export function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>): QueryResult<T, K, I>
export function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]): QueryResult<T, K, I> {
  const [result, setResult] = useState<QueryResult<T, K, I>>({contracts: [], loading: false});
  const state = useDamlState();
  useEffect(() => {
    setResult({contracts: [], loading: true});
    const query = queryFactory ? queryFactory() : undefined;
    console.debug(`mount useStreamQuery(${template.templateId}, ...)`, query);
    const stream = state.ledger.streamQuery(template, query);
    stream.on('live', () => setResult(result => ({...result, loading: false})));
    stream.on('change', contracts => setResult(result => ({...result, contracts})));
    stream.on('close', closeEvent => {
      console.error('useStreamQuery: web socket closed', closeEvent);
      setResult(result => ({...result, loading: true}));
    });
    return (): void => {
      console.debug(`unmount useStreamQuery(${template.templateId}, ...)`, query);
      stream.close();
    };
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, template, ...(queryDeps ?? [])]);
  return result;
}

/**
 * React Hook to query the ledger. Same as useStreamQuery, but query by contract key instead.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to match.
 * @param queryFactory A function returning a contract key.
 * @param queryDeps The dependencies of the query (for which a change triggers an update of the result)
 *
 * @return The matching (unique) contract.
 */
export function useStreamFetchByKey<T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]): FetchResult<T, K, I> {
  const [result, setResult] = useState<FetchResult<T, K, I>>({contract: null, loading: false});
  const state = useDamlState();
  useEffect(() => {
    setResult({contract: null, loading: true});
    const key = keyFactory();
    console.debug(`mount useStreamFetchByKey(${template.templateId}, ...)`, key);
    const stream = state.ledger.streamFetchByKey(template, key);
    stream.on('change', contract => setResult(result => ({...result, contract})));
    stream.on('close', closeEvent => {
      console.error('useStreamFetchByKey: web socket closed', closeEvent);
      setResult(result => ({...result, loading: true}));
    });
    setResult(result => ({...result, loading: false}));
    return (): void => {
      console.debug(`unmount useStreamFetchByKey(${template.templateId}, ...)`, key);
      stream.close();
    };
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, template, ...keyDeps]);
  return result;
}

/**
 * React Hook to reload all active queries.
 */
export const useReload = (): () => void => {
  const state = useDamlState();
  return (): void => state.triggerReload();
}
