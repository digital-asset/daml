// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Choice, ContractId, Template } from "@daml/types";
import { CreateEvent, Query } from '@daml/ledger';
import { useState, useContext, useEffect } from "react";
import { DamlLedgerState, DamlLedgerContext } from './context'

// NOTE(MH, useEffect dependencies): There are various places in this file
// where we need to maintain the dependencies of the `useEffect` hook manually
// and there's no tool to help us enfore they are correct. Thus, we need to be
// extra careful in these locations. If we add too many dependencies, we will
// make unnecessary network requests. If we forget adding some dependencies, we
// not make a new network request although they are required to refresh data.


const useDamlState = (): DamlLedgerState => {
  const state = useContext(DamlLedgerContext);
  if (!state) {
    throw Error("Trying to use DamlLedgerContext before initializing.")
  }
  return state;
}

export const useParty = () => {
  const state = useDamlState();
  return state.party;
}

export type QueryResult<T extends object, K, I extends string> = {
  contracts: readonly CreateEvent<T, K, I>[];
  loading: boolean;
}

/// React Hook for a query against the `/v1/query` endpoint of the JSON API.
export function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory: () => Query<T>, queryDeps: readonly unknown[]): QueryResult<T, K, I>
export function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>): QueryResult<T, K, I>
export function useQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]): QueryResult<T, K, I> {
  const state = useDamlState();
  const [result, setResult] = useState<QueryResult<T, K, I>>({contracts: [], loading: false});
  useEffect(() => {
    setResult({contracts: [], loading: true});
    const query = queryFactory ? queryFactory() : undefined;
    const load = async () => {
      const contracts = await state.ledger.query(template, query);
      setResult({contracts, loading: false});
    };
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    load();
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, state.reloadToken, template, ...(queryDeps ?? [])]);
  return result;
}

export type FetchResult<T extends object, K, I extends string> = {
  contract: CreateEvent<T, K, I> | null;
  loading: boolean;
}

/// React Hook for a lookup by key against the `/v1/fetch` endpoint of the JSON API.
export function useFetchByKey<T extends object, K, I extends string>(template: Template<T, K, I>, keyFactory: () => K, keyDeps: readonly unknown[]): FetchResult<T, K, I> {
  const state = useDamlState();
  const [result, setResult] = useState<FetchResult<T, K, I>>({contract: null, loading: false});
  useEffect(() => {
    const key = keyFactory();
    setResult({contract: null, loading: true});
    const load = async () => {
      const contract = await state.ledger.fetchByKey(template, key);
      setResult({contract, loading: false});
    };
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    load();
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, state.reloadToken, template, ...(keyDeps ?? [])]);
  return result;
}

/// React Hook that returns a function to exercise a choice by contract id.
export const useExercise = <T extends object, C, R>(choice: Choice<T, C, R>): (cid: ContractId<T>, argument: C) => Promise<R> => {
  const state = useDamlState();
  const exercise = async (cid: ContractId<T>, argument: C) => {
    const [result] = await state.ledger.exercise(choice, cid, argument);
    return result;
  }
  return exercise;
}

/// React Hook that returns a function to exercise a choice by key.
export const useExerciseByKey = <T extends object, C, R, K>(choice: Choice<T, C, R, K>): (key: K, argument: C) => Promise<R> => {
  const state = useDamlState();
  const exerciseByKey = async (key: K, argument: C) => {
    const [result] = await state.ledger.exerciseByKey(choice, key, argument);
    return result;
  }
  return exerciseByKey;
}

/// React Hook for a query against the `/v1/stream/query` endpoint of the JSON API.
export function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>): QueryResult<T, K, I>
export function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory: () => Query<T>, queryDeps: readonly unknown[]): QueryResult<T, K, I>
export function useStreamQuery<T extends object, K, I extends string>(template: Template<T, K, I>, queryFactory?: () => Query<T>, queryDeps?: readonly unknown[]): QueryResult<T, K, I> {
  const [result, setResult] = useState<QueryResult<T, K, I>>({contracts: [], loading: false});
  const state = useDamlState();
  useEffect(() => {
    setResult({contracts: [], loading: true});
    const query = queryFactory ? queryFactory() : undefined;
    console.debug(`mount useStreamQuery(${template.templateId}, ...)`, query);
    const stream = state.ledger.streamQuery(template, query);
    stream.on('change', contracts => setResult(result => ({...result, contracts})));
    stream.on('close', closeEvent => {
      console.error('useStreamQuery: web socket closed', closeEvent);
      setResult(result => ({...result, loading: true}));
    });
    setResult(result => ({...result, loading: false}));
    return () => {
      console.debug(`unmount useStreamQuery(${template.templateId}, ...)`, query);
      stream.close();
    };
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, template, ...(queryDeps ?? [])]);
  return result;
}

/// React Hook for a query against the `/v1/stream/fetch` endpoint of the JSON API.
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
    return () => {
      console.debug(`unmount useStreamFetchByKey(${template.templateId}, ...)`, key);
      stream.close();
    };
  // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
  }, [state.ledger, template, ...keyDeps]);
  return result;
}

/// React Hook to reload all queries currently present in the store.
export const useReload = (): () => void => {
  const state = useDamlState();
  return () => state.triggerReload();
}
