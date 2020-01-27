// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as immutable from 'immutable';
import { Template, lookupTemplate } from '@daml/types';
import { CreateEvent, Query, Event } from '@daml/ledger';
import * as TemplateStore from './templateStore';

export type Store = {
  templateStores: immutable.Map<Template<object, unknown>, TemplateStore.Store<object, unknown>>;
}

export const empty = (): Store => ({
  templateStores: immutable.Map(),
});

export const setTemplateLoading = <T extends object>(store: Store, template: Template<T>) => ({
  ...store,
  templateStores: store.templateStores.update(template, TemplateStore.setAllLoading)
});

export const getQueryResult = <T extends object, K>(store: Store, template: Template<T, K>, query: Query<T>): TemplateStore.QueryResult<T, K> | undefined => {
  const templateStore = store.templateStores.get(template) as TemplateStore.Store<T, K> | undefined;
  return templateStore?.queryResults.get(query);
}

export const setQueryLoading = <T extends object>(store: Store, template: Template<T>, query: Query<T>): Store => ({
  ...store,
  templateStores: store.templateStores.update(template, (templateStore = TemplateStore.empty()) =>
    TemplateStore.setQueryLoading(templateStore, query))
});

export const setQueryResult = <T extends object>(store: Store, template: Template<T>, query: Query<T>, contracts: CreateEvent<T>[]): Store => ({
  ...store,
  templateStores: store.templateStores.update(template, (templateStore = TemplateStore.empty()) =>
    TemplateStore.setQueryResult(templateStore, query, contracts))
});

export const getFetchByKeyResult = <T extends object, K>(store: Store, template: Template<T, K>, key: K): TemplateStore.FetchResult<T, K> | undefined => {
  const templateStore = store.templateStores.get(template) as TemplateStore.Store<T, K> | undefined;
  return templateStore?.fetchByKeyResults.get(key);
}

export const setFetchByKeyLoading = <T extends object, K>(store: Store, template: Template<T, K>, key: K): Store => ({
  ...store,
  templateStores: store.templateStores.update(template, (templateStore = TemplateStore.empty()) =>
    TemplateStore.setFetchByKeyLoading(templateStore, key))
});

export const setFetchByKeyResult = <T extends object, K>(store: Store, template: Template<T>, key: K, contract: CreateEvent<T, K> | null): Store => ({
  ...store,
  templateStores: store.templateStores.update(template, (templateStore = TemplateStore.empty()) =>
    TemplateStore.setFetchByKeyResult(templateStore, key, contract))
});

export const addEvents = (store: Store, events: Event<object>[]): Store => {
  const eventsByTemplateId = immutable.List(events).groupBy((event) =>
    'created' in event ? event.created.templateId : event.archived.templateId);
  let templateStores = store.templateStores;
  eventsByTemplateId.forEach((events, templateId) => {
    const template = lookupTemplate(templateId);
    if (templateStores.has(template)) {
      templateStores = templateStores.update(template, (templateStore) =>
        TemplateStore.addEvents(templateStore, events.valueSeq().toArray()));
    }
  });
  return {templateStores};
}
