// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import React, { ComponentType } from 'react';
import { renderHook } from '@testing-library/react-hooks';
import DamlLedger from './DamlLedger'
import { useQuery } from './hooks';
import { Template } from '@daml/types';

let mockResolveQuery: (contracts: string[]) => void = () => {
  throw Error('using mockResolveQuery before init');
}

jest.mock('@daml/ledger', () => class {
  query(): Promise<string[]> {
    return new Promise((resolve) => mockResolveQuery = resolve);
  }
});

const Foo = undefined as unknown as Template<object>;

test('useQuery', async () => {
  const wrapper: ComponentType = ({children}) => React.createElement(DamlLedger, {token: 'token', party: 'party'}, children);
  const {result, waitForNextUpdate} = renderHook(() => useQuery(Foo), {wrapper});
  expect(result.current.contracts).toEqual([]);
  expect(result.current.loading).toBe(true);
  const resolvent = ['foo'];
  mockResolveQuery(resolvent);
  await waitForNextUpdate();
  expect(result.current.contracts).toBe(resolvent);
  expect(result.current.loading).toBe(false);
});
