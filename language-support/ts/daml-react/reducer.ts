// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Template } from "@daml/types";
import { CreateEvent, Query, Event } from '@daml/ledger';
import * as LedgerStore from './ledgerStore';

enum ActionType {
  SetQueryLoading,
  SetQueryResult,
  SetFetchByKeyLoading,
  SetFetchByKeyResult,
  AddEvents,
}

type SetQueryLoadingAction<T extends object> = {
  type: typeof ActionType.SetQueryLoading;
  template: Template<T>;
  query: Query<T>;
}

type SetQueryResultAction<T extends object> = {
  type: typeof ActionType.SetQueryResult;
  template: Template<T>;
  query: Query<T>;
  contracts: CreateEvent<T>[];
}

type SetFetchByKeyLoadingAction<T extends object, K> = {
  type: typeof ActionType.SetFetchByKeyLoading;
  template: Template<T, K>;
  key: K;
}

type SetFetchByKeyResultAction<T extends object, K> = {
  type: typeof ActionType.SetFetchByKeyResult;
  template: Template<T, K>;
  key: K;
  contract: CreateEvent<T, K> | null;
}

type AddEventsAction = {
  type: typeof ActionType.AddEvents;
  events: Event<object>[];
}

export type Action =
  | SetQueryLoadingAction<object>
  | SetQueryResultAction<object>
  | SetFetchByKeyLoadingAction<object, unknown>
  | SetFetchByKeyResultAction<object, unknown>
  | AddEventsAction

export const setQueryLoading = <T extends object>(template: Template<T>, query: Query<T>): SetQueryLoadingAction<T> => ({
  type: ActionType.SetQueryLoading,
  template,
  query,
});

export const setQueryResult = <T extends object>(template: Template<T>, query: Query<T>, contracts: CreateEvent<T>[]): SetQueryResultAction<T> => ({
  type: ActionType.SetQueryResult,
  template,
  query,
  contracts,
});

export const setFetchByKeyLoading = <T extends object, K>(template: Template<T, K>, key: K): SetFetchByKeyLoadingAction<T, K> => ({
  type: ActionType.SetFetchByKeyLoading,
  template,
  key,
});

export const setFetchByKeyResult = <T extends object, K>(template: Template<T, K>, key: K, contract: CreateEvent<T, K> | null): SetFetchByKeyResultAction<T, K> => ({
  type: ActionType.SetFetchByKeyResult,
  template,
  key,
  contract,
});

export const addEvents= (events: Event<object>[]): AddEventsAction => ({
  type: ActionType.AddEvents,
  events,
});

export const reducer = (ledgerStore: LedgerStore.Store, action: Action): LedgerStore.Store => {
  switch (action.type) {
    case ActionType.SetQueryLoading: {
      return LedgerStore.setQueryLoading(ledgerStore, action.template, action.query);
    }
    case ActionType.SetQueryResult: {
      return LedgerStore.setQueryResult(ledgerStore, action.template, action.query, action.contracts);
    }
    case ActionType.SetFetchByKeyLoading: {
      return LedgerStore.setFetchByKeyLoading(ledgerStore, action.template, action.key);
    }
    case ActionType.SetFetchByKeyResult: {
      return LedgerStore.setFetchByKeyResult(ledgerStore, action.template, action.key, action.contract);
    }
    case ActionType.AddEvents: {
      return LedgerStore.addEvents(ledgerStore, action.events);
    }
  }
}
