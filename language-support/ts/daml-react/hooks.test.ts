// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import React, { ComponentType } from 'react';
import { renderHook, RenderHookResult } from '@testing-library/react-hooks';
import DamlLedger, { useQuery} from './index';
import { Template } from '@daml/types';
import { useParty } from './hooks';

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

test('useQuery', async () => {
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
