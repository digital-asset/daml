// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React, {useContext, useEffect, useMemo, useState } from 'react';
import { ContractId,Party, Template } from '@daml/types';
import Ledger, { CreateEvent, Query } from '@daml/ledger';

/**
 * @internal
 */
type DamlLedgerState = {
  reloadToken: unknown;
  triggerReload: () => void;
  party: Party;
  ledger: Ledger;
}

/**
 * React props to initiate a connect to a DAML ledger.
 */
export type LedgerProps = {
  token: string;
  httpBaseUrl?: string;
  wsBaseUrl?: string;
  party: Party;
}

/**
 * The result of a ``query`` against the ledger.
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
 * A LedgerContext is a React context that stores information about a DAML Ledger
 * and hooks necessary to use it.
 */
export type LedgerContext = {
  DamlLedger: React.FC<LedgerProps>;
  useParty: () => Party;
  useLedger: () => Ledger;
  useQuery: <T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]) => QueryResult<T, K, I>;
  useFetch: <T extends object, K, I extends string>(template: Template<T, K, I>, contractId: ContractId<T>) => FetchResult<T, K, I>;
  useFetchByKey: <T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]) => FetchResult<T, K, I>;
  useStreamQuery: <T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]) => QueryResult<T, K, I>;
  useStreamFetchByKey: <T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]) => FetchResult<T, K, I>;
  useReload: () => () => void;
}

/**
 * Create a [[LedgerContext]]. One should use this function, instead of the default [[DamlLedger]],
 * where one needs to be able to nest ledger interactions, by different parties or connections, within
 * one React application.
 *
 * @param contextName Used to refer to a context in case of errors.
 */
export function createLedgerContext(contextName="DamlLedgerContext"): LedgerContext {

  // NOTE(MH, useEffect dependencies): There are various places in this file
  // where we need to maintain the dependencies of the `useEffect` hook manually
  // and there's no tool to help us enfore they are correct. Thus, we need to be
  // extra careful in these locations. If we add too many dependencies, we will
  // make unnecessary network requests. If we forget adding some dependencies, we
  // not make a new network request although they are required to refresh data.

  const ledgerContext = React.createContext<DamlLedgerState | undefined>(undefined);
  const DamlLedger: React.FC<LedgerProps> = ({token, httpBaseUrl, wsBaseUrl, party, children}) => {
    const [reloadToken, setReloadToken] = useState(0);
    const ledger = useMemo(() => new Ledger({token, httpBaseUrl, wsBaseUrl}), [token, httpBaseUrl, wsBaseUrl]);
    const state: DamlLedgerState = useMemo(() => ({
      reloadToken,
      triggerReload: (): void => setReloadToken(x => x + 1),
      party,
      ledger,
    }), [party, ledger, reloadToken]);
    return React.createElement(ledgerContext.Provider, {value: state}, children);
  }

  const useDamlState = (): DamlLedgerState => {
    const state = useContext(ledgerContext);
    if (!state) {
      throw Error(`Trying to use ${contextName} before initializing.`);
    }
    return state;
  }

  const useParty = (): Party => {
    const state = useDamlState();
    return state.party;
  }

  const useLedger = (): Ledger => {
    return useDamlState().ledger;
  }

  function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]): QueryResult<T, K, I> {
    const state = useDamlState();
    const [result, setResult] = useState<QueryResult<T, K, I>>({contracts: [], loading: true});
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

  function useFetch<T extends object, K, I extends string>(template: Template<T, K, I>, contractId: ContractId<T>): FetchResult<T, K, I> {
    const state = useDamlState();
    const [result, setResult] = useState<FetchResult<T, K, I>>({contract: null, loading: true});
    useEffect(() => {
      setResult({contract: null, loading: true});
      const load = async (): Promise<void> => {
        const contract = await state.ledger.fetch(template, contractId);
        setResult({contract, loading: false});
      };
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      load();
    // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
    }, [state.ledger, state.reloadToken, template, contractId]);
    return result;
  }

  function useFetchByKey<T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]): FetchResult<T, K, I> {
    const state = useDamlState();
    const [result, setResult] = useState<FetchResult<T, K, I>>({contract: null, loading: true});
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

  function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]): QueryResult<T, K, I> {
    const [result, setResult] = useState<QueryResult<T, K, I>>({contracts: [], loading: true});
    const state = useDamlState();
    useEffect(() => {
      setResult({contracts: [], loading: true});
      const query = queryFactory ? [queryFactory()] : [];
      console.debug(`mount useStreamQuery(${template.templateId}, ...)`, query);
      const stream = state.ledger.streamQueries(template, query);
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

  function useStreamFetchByKey<T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]): FetchResult<T, K, I> {
    const [result, setResult] = useState<FetchResult<T, K, I>>({contract: null, loading: true});
    const state = useDamlState();
    useEffect(() => {
      setResult({contract: null, loading: true});
      const key = keyFactory();
      console.debug(`mount useStreamFetchByKey(${template.templateId}, ...)`, key);
      const stream = state.ledger.streamFetchByKeys(template, [key]);
      stream.on('change', contracts => setResult(result => ({...result, contract: contracts[0]})));
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

  const useReload = (): () => void => {
    const state = useDamlState();
    return (): void => state.triggerReload();
  }

  return { DamlLedger, useParty, useLedger, useQuery, useFetch, useFetchByKey, useStreamQuery, useStreamFetchByKey, useReload };
}
