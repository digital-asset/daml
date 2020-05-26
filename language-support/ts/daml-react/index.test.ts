// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// NOTE(MH): Unfortunately the `act` function triggers this warning by looking
// like a promis without being one.
/* eslint-disable @typescript-eslint/no-floating-promises */
import React, { ComponentType, useState } from 'react';
import { renderHook, RenderHookResult, act } from '@testing-library/react-hooks';
import DamlLedger, { useParty, useQuery, useFetchByKey, useStreamQuery, useStreamFetchByKey, useReload } from './index';
import { Template } from '@daml/types';
import { Stream } from '@daml/ledger';
import {EventEmitter} from 'events';

const mockConstructor = jest.fn();
const mockQuery = jest.fn();
const mockFetchByKey = jest.fn();
const mockStreamQuery = jest.fn();
const mockStreamFetchByKey = jest.fn();
const mockFunctions = [mockConstructor, mockQuery, mockFetchByKey, mockStreamQuery, mockStreamFetchByKey];

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

  streamQuery(...args: unknown[]): Stream<object, string, string, string[]>{
    return mockStreamQuery(...args);
  }

  streamFetchByKey(...args: unknown[]): Stream<object, string, string, string | null>{
    return mockStreamFetchByKey(...args);
  }
});

/**
 * Returns a mock stream object using an `EventEmitter` to implement on, off functions.
 */
const mockStream = <T>(): [Stream <object, string, string, T>, EventEmitter] =>
{
  const emitter = new EventEmitter();
  const stream =
    {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      on: (type: string, listener: (...args: any[]) => void): void => void emitter.on(type, listener),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      off: (type: string, listener: (...args: any[]) => void): void => void emitter.on(type, listener),
      close: (): void => {
        emitter.removeAllListeners();
        console.log('mock stream closed');
      }

    };
  return [stream, emitter];
};

const TOKEN = 'test_token';
const PARTY = 'test_party';

function renderDamlHook<P, R>(callback: (props: P) => R): RenderHookResult<P, R> {
  const wrapper: ComponentType = ({children}) => React.createElement(DamlLedger, {token: TOKEN, party: PARTY}, children);
  return renderHook(callback, {wrapper});
}

/**
 * Dummy template, needs at least the templateId field for debug messages emitted by
 * `useStreamQuery`.
 */
const Foo = {templateId: 'FooTemplateId'} as unknown as Template<object>;

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

  test('useReload', async () => {
    const resolvent1 = ['foo'];
    const resolvent2 = ['bar'];
    mockQuery.mockReturnValueOnce(Promise.resolve(resolvent1));
    const {result, waitForNextUpdate} = renderDamlHook(() => {
      const reload = useReload();
      const queryResult = useQuery(Foo);
      return {reload, queryResult};
    }
    );
    // first query
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenLastCalledWith(Foo, undefined);
    await waitForNextUpdate();
    expect(result.current.queryResult).toEqual({contracts : resolvent1, loading : false});
    mockQuery.mockClear();
    // query result changes
    mockQuery.mockReturnValueOnce(Promise.resolve(resolvent2));
    // user reloads
    act(() => result.current.reload());
    await waitForNextUpdate();
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery).toHaveBeenLastCalledWith(Foo, undefined);
    expect(result.current.queryResult).toEqual({contracts : resolvent2, loading : false});
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

  test('useReload', async () => {
    const contract1 = {owner: 'Alice'};
    const key1 = contract1.owner;
    const contract2 = {owner: 'Bob'};
    mockFetchByKey.mockReturnValueOnce(Promise.resolve(contract1));
    const {result, waitForNextUpdate} = renderDamlHook(() => {
      const reload = useReload();
      const fetchResult = useFetchByKey(Foo, () => key1, [key1]);
      return {reload, fetchResult};
    }
    );
    // first fetchByKey
    expect(mockFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockFetchByKey).toHaveBeenLastCalledWith(Foo, key1);
    await waitForNextUpdate();
    expect(result.current.fetchResult).toEqual({contract: contract1, loading: false});
    mockFetchByKey.mockClear();

    //  fetchByKey result changes
    mockFetchByKey.mockReturnValueOnce(Promise.resolve(contract2));
    // user reloads
    act(() => result.current.reload());
    await waitForNextUpdate();
    expect(mockFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockFetchByKey).toHaveBeenLastCalledWith(Foo, key1);
    expect(result.current.fetchResult).toEqual({contract: contract2, loading: false});
  });
});

describe('useStreamQuery', () => {
  test('live event changes loading status', () => {
    // setup
    const query = 'foo-query';
    const [stream, emitter] = mockStream();
    mockStreamQuery.mockReturnValueOnce(stream);
    const hookResult = renderDamlHook(() => useStreamQuery(Foo, () => ({query}), [query]));
    expect(mockStreamQuery).toHaveBeenCalledTimes(1);
    expect(mockStreamQuery).toHaveBeenLastCalledWith(Foo, {query});

    // no events have been emitted.
    expect(hookResult.result.current).toEqual({contracts: [], loading:true});

    // live event
    act(() =>
      void emitter.emit('live', [])
    );
    expect(hookResult.result.current).toEqual({contracts: [], loading: false});
  });

  test('live event changes loading status', () => {
    // setup
    const query = 'foo-query';
    const [stream, emitter] = mockStream();
    mockStreamQuery.mockReturnValueOnce(stream);
    const hookResult = renderDamlHook(() => useStreamQuery(Foo, () => ({query}), [query]));
    expect(mockStreamQuery).toHaveBeenCalledTimes(1);
    expect(mockStreamQuery).toHaveBeenLastCalledWith(Foo, {query});

    // no events have been emitted.
    expect(hookResult.result.current).toEqual({contracts: [], loading:true});

    // live event
    act(() =>
      void emitter.emit('live', [])
    );
    expect(hookResult.result.current).toEqual({contracts: [], loading: false});
  });

  test('empty stream', () => {
    // setup
    const query = 'foo-query';
    const [stream, emitter] = mockStream();
    mockStreamQuery.mockReturnValueOnce(stream);
    const hookResult = renderDamlHook(() => useStreamQuery(Foo, () => ({query}), [query]));
    expect(mockStreamQuery).toHaveBeenCalledTimes(1);
    expect(mockStreamQuery).toHaveBeenLastCalledWith(Foo, {query});

    // live event
    act(() =>
      void emitter.emit('live', [])
    );

    // no events have been emitted.
    expect(hookResult.result.current).toEqual({contracts: [], loading:false});

    // empty events
    act(() =>
      void emitter.emit('change', [])
    );
    expect(hookResult.result.current).toEqual({contracts: [], loading: false});
  });


  test('new events', () => {
    // setup
    const query = 'foo-query';
    const [stream, emitter] = mockStream();
    mockStreamQuery.mockReturnValueOnce(stream);
    const hookResult = renderDamlHook(() => useStreamQuery(Foo, () => ({query}), [query]));
    expect(mockStreamQuery).toHaveBeenCalledTimes(1);
    expect(mockStreamQuery).toHaveBeenLastCalledWith(Foo, {query: query});
    expect(hookResult.result.current.contracts).toEqual([]);

    // live event
    act(() =>
      void emitter.emit('live', [])
    );

    expect(hookResult.result.current.loading).toBe(false);

    // one new event
    act(() => void emitter.emit('change', ['event1']));
    expect(hookResult.result.current.contracts).toEqual(['event1']);
    expect(hookResult.result.current.loading).toBe(false);

    // two new events replacing old one.
    act(() => void emitter.emit('change', ['event2', 'event3']));
    expect(hookResult.result.current).toEqual({contracts: ['event2', 'event3'], loading: false});
  });

  test('changed query triggers reload', () => {
    // setup
    const query1 = 'foo-query';
    const query2 = 'bar-query';
    const [stream, emitter] = mockStream();
    mockStreamQuery.mockReturnValueOnce(stream);
    const {result} = renderDamlHook(() => {
      const [query, setQuery] = useState(query1);
      const queryResult = useStreamQuery(Foo, () => ({query}), [query]);
      return {queryResult, query, setQuery};
    })
    expect(mockStreamQuery).toHaveBeenCalledTimes(1);
    expect(mockStreamQuery).toHaveBeenLastCalledWith(Foo, {query: query1});

    // live event
    act(() =>
      void emitter.emit('live', [])
    );

    expect(result.current.queryResult).toEqual({contracts: [], loading: false});

    // new events
    act(() => void emitter.emit('change', ['foo']));
    expect(result.current.queryResult).toEqual({contracts: ['foo'], loading: false});

    // change query, expect stream to have been called with new query.
    mockStreamQuery.mockClear();
    mockStreamQuery.mockReturnValueOnce(stream);
    act(() => result.current.setQuery(query2));
    // live event
    act(() => void emitter.emit('live', null));
    // change event
    act(() => void emitter.emit('change', ['bar']));
    expect(mockStreamQuery).toHaveBeenCalledTimes(1);
    expect(mockStreamQuery).toHaveBeenLastCalledWith(Foo, {query: query2});
    expect(result.current.queryResult).toEqual({contracts: ['bar'], loading: false});
  });
});

describe('useStreamFetchByKey', () => {
  test('empty stream', () => {
    const contract = {owner: 'Alice'};
    const key = contract.owner;
    const [stream, emitter] = mockStream();
    mockStreamFetchByKey.mockReturnValueOnce(stream);
    const {result} = renderDamlHook(() => useStreamFetchByKey(Foo, () => key, [key]));
    expect(mockStreamFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockStreamFetchByKey).toHaveBeenLastCalledWith(Foo, key);
    expect(result.current).toEqual({contract: null, loading: false});

    act(() => void emitter.emit('change', null));
    expect(result.current).toEqual({contract: null, loading: false});
  }),

  test('new event', () => {
    const contract = {owner: 'Alice'};
    const key = contract.owner;
    const [stream, emitter] = mockStream();
    mockStreamFetchByKey.mockReturnValueOnce(stream);
    const {result} = renderDamlHook(() => useStreamFetchByKey(Foo, () => key, [key]));
    expect(mockStreamFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockStreamFetchByKey).toHaveBeenLastCalledWith(Foo, key);
    expect(result.current).toEqual({contract: null, loading: false});

    act(() => void emitter.emit('change', contract));
    expect(result.current).toEqual({contract: contract, loading: false});
  }),

  test('changed key triggers reload', () => {
    const contract = {k1 : 'Alice', k2: 'Bob'};
    const key1 = contract.k1;
    const key2 = contract.k2
    const [stream, emitter] = mockStream();
    mockStreamFetchByKey.mockReturnValueOnce(stream);
    const {result} = renderDamlHook(() => {
      const [key, setKey] = useState(key1);
      const fetchResult = useStreamFetchByKey(Foo, () => key, [key]);
      return {fetchResult, key, setKey};
    })
    act(() => void emitter.emit('change', contract));
    expect(mockStreamFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockStreamFetchByKey).toHaveBeenLastCalledWith(Foo, key1);
    expect(result.current.fetchResult).toEqual({contract: contract, loading: false});

    mockStreamFetchByKey.mockClear();
    mockStreamFetchByKey.mockReturnValueOnce(stream);
    act(() => result.current.setKey(key2));
    expect(mockStreamFetchByKey).toHaveBeenCalledTimes(1);
    expect(mockStreamFetchByKey).toHaveBeenLastCalledWith(Foo, key2);
  })
});
