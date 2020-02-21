// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// NOTE(MH): Unfortunately the `act` function triggers this warning by looking
// like a promis without being one.
/* eslint-disable @typescript-eslint/no-floating-promises */
import React, { ComponentType, useState } from 'react';
import { renderHook, RenderHookResult, act } from '@testing-library/react-hooks';
import DamlLedger, { useParty, useQuery } from './index';
import { Template } from '@daml/types';

const mockConstructor = jest.fn();
const mockQuery = jest.fn();
const mockFunctions = [mockConstructor, mockQuery];

jest.mock('@daml/ledger', () => class {
  constructor(...args: unknown[]) {
    mockConstructor(...args);
  }
  query(...args: unknown[]): Promise<string> {
    return mockQuery(...args);
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
});
