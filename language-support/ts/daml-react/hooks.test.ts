// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// NOTE(MH): Unfortunately the `act` function triggers this warning by looking
// like a promis without being one.
/* eslint-disable @typescript-eslint/no-floating-promises */
import React, { ComponentType, useState } from 'react';
import { renderHook, RenderHookResult, act } from '@testing-library/react-hooks';
import DamlLedger, { useParty, useQuery, useFetchByKey } from './index';
import Ledger from '@daml/ledger';
import { Template } from '@daml/types';
import {useLedger} from './hooks';

const mockConstructor = jest.fn();
const mockQuery = jest.fn();
const mockFetchByKey = jest.fn();
const mockFunctions = [mockConstructor, mockQuery, mockFetchByKey];

jest.mock('@daml/ledger', () => class {
  constructor(...args: unknown[]) {
    mockConstructor(...args);
  }
  query(...args: unknown[]): Promise<string> {
    return mockQuery(...args);
  }

  fetchByKey(...args: unknown[]): Promise<string> {
    return mockFetchByKey(...args);
  }
});

const TOKEN = 'test_token';
const PARTY = 'test_party';

function renderDamlHook<P, R>(callback: (props: P) => R): RenderHookResult<P, R> {
  const wrapper: ComponentType = ({children}) => React.createElement(DamlLedger, {token: TOKEN, party: PARTY}, children);
  return renderHook(callback, {wrapper});
}

const Foo = undefined as unknown as Template<object>;

beforeEach(() => {
  mockFunctions.forEach(mock => mock.mockClear());
});

test('DamlLedger', () => {
  renderDamlHook(() => { return; });
  expect(mockConstructor).toHaveBeenCalledTimes(1);
  expect(mockConstructor).toHaveBeenLastCalledWith({token: TOKEN, httpBaseUrl: undefined, wsBaseUrl: undefined});
});

test('useParty', () => {
  const {result} = renderDamlHook(() => useParty());
  expect(result.current).toBe(PARTY);
});

test('useLedger', () => {
  const {result} = renderDamlHook(() => useLedger())
  expect(result.current).toBeInstanceOf(Ledger);
});

describe('useQuery', () => {
  test('one shot without query', async () => {
    const resolvent = ['foo'];
    mockQuery.mockReturnValueOnce(Promise.resolve(resolvent));
    const {result, waitForNextUpdate} = renderDamlHook(() => useQuery(Foo));
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenLastCalledWith(Foo, undefined);
    mockQuery.mockClear();
    expect(result.current.contracts).toEqual([]);
    expect(result.current.loading).toBe(true);
    await waitForNextUpdate();
    expect(mockQuery).not.toHaveBeenCalled();
    expect(result.current.contracts).toBe(resolvent);
    expect(result.current.loading).toBe(false);
  });

  test('change to query', async () => {
    const query1 = 'foo-query';
    const query2 = 'bar-query';
    const resolvent1 = ['foo'];
    const resolvent2 = ['bar'];

    // First rendering works?
    mockQuery.mockReturnValueOnce(Promise.resolve(resolvent1));
    const {result, waitForNextUpdate} = renderDamlHook(() => {
      const [query, setQuery] = useState(query1);
      const queryResult = useQuery(Foo, () => ({query}), [query]);
      return {queryResult, query, setQuery};
    });
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenLastCalledWith(Foo, {query: query1});
    mockQuery.mockClear();
    expect(result.current.queryResult).toEqual({contracts: [], loading: true});
    expect(result.current.query).toBe(query1);
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contracts: resolvent1, loading: false});

    // Change to query triggers another call to JSON API?
    mockQuery.mockReturnValueOnce(Promise.resolve(resolvent2));
    act(() => result.current.setQuery(query2));
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenLastCalledWith(Foo, {query: query2});
    mockQuery.mockClear();
    expect(result.current.queryResult).toEqual({contracts: [], loading: true});
    expect(result.current.query).toBe(query2);
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contracts: resolvent2, loading: false});
  });

  test('rerendering without query change', async () => {
    const query = 'query';
    const resolvent = ['foo'];

    // First rendering works?
    mockQuery.mockReturnValueOnce(Promise.resolve(resolvent));
    const {result, waitForNextUpdate} = renderDamlHook(() => {
      const setState = useState('state')[1];
      const queryResult = useQuery(Foo, () => ({query}), [query]);
      return {queryResult, setState};
    });
    expect(mockQuery).toHaveBeenCalledTimes(1);
    mockQuery.mockClear();
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contracts: resolvent, loading: false});

    // Change to unrelated state does _not_ trigger another call to JSON API?
    act(() => result.current.setState('new-state'));
    expect(mockQuery).not.toHaveBeenCalled();
    expect(result.current.queryResult).toEqual({contracts: resolvent, loading: false});
  });
});

describe('useFetchByKey', () => {
  test('one shot', async () => {
    const contract = {owner: 'Alice'};
    const key = contract.owner;
    mockFetchByKey.mockReturnValueOnce(Promise.resolve(contract));
    const {result, waitForNextUpdate} = renderDamlHook(() => useFetchByKey(Foo, () => key, [key]));
    expect(mockFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockFetchByKey).toHaveBeenLastCalledWith(Foo, key);
    mockFetchByKey.mockClear();
    expect(result.current).toEqual({contract: null, loading: true});
    await waitForNextUpdate();
    expect(mockFetchByKey).not.toHaveBeenCalled();
    expect(result.current).toEqual({contract, loading: false});
  });

  test('change to key', async () => {
    const contract1 = {owner: 'Alice'};
    const key1 = contract1.owner;
    const contract2 = {owner: 'Bob'};
    const key2 = contract2.owner;

    // First rendering works?
    mockFetchByKey.mockReturnValueOnce(Promise.resolve(contract1));
    const {result, waitForNextUpdate} = renderDamlHook(() => {
      const [key, setKey] = useState(key1);
      const queryResult = useFetchByKey(Foo, () => key, [key]);
      return {queryResult, key, setKey};
    });
    expect(mockFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockFetchByKey).toHaveBeenLastCalledWith(Foo, key1);
    mockFetchByKey.mockClear();
    expect(result.current.queryResult).toEqual({contract: null, loading: true});
    expect(result.current.key).toBe(key1);
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contract: contract1, loading: false});

    // Change to key triggers another call to JSON API?
    mockFetchByKey.mockReturnValueOnce(Promise.resolve(contract2));
    act(() => result.current.setKey(key2));
    expect(mockFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockFetchByKey).toHaveBeenLastCalledWith(Foo, key2);
    mockFetchByKey.mockClear();
    expect(result.current.queryResult).toEqual({contract: null, loading: true});
    expect(result.current.key).toBe(key2);
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contract: contract2, loading: false});
  });

  test('rerendering without key change', async () => {
    const contract = {owner: 'Alice'};
    const key = contract.owner;

    // First rendering works?
    mockFetchByKey.mockReturnValueOnce(Promise.resolve(contract));
    const {result, waitForNextUpdate} = renderDamlHook(() => {
      const setState = useState('state')[1];
      const queryResult = useFetchByKey(Foo, () => key, [key]);
      return {queryResult, setState};
    });
    expect(mockFetchByKey).toHaveBeenCalledTimes(1);
    mockFetchByKey.mockClear();
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contract, loading: false});

    // Change to unrelated state does _not_ trigger another call to JSON API?
    act(() => result.current.setState('new-state'));
    expect(mockFetchByKey).not.toHaveBeenCalled();
    expect(result.current.queryResult).toEqual({contract, loading: false});
  });
});
