// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {ContractId, registerTemplate} from '@daml/types'
import {Event} from '@daml/ledger'
import * as jtv from '@mojotech/json-type-validation'
import * as LedgerStore from './ledgerStore'

// mock data
const templateId = "A.B.C"
const template = {  templateId: templateId
                  , Archive: {  template: () => template
                              , choiceName: 'Archive'
                              , argumentDecoder: jtv.anyJson
                              , resultDecoder: jtv.anyJson
                              }
                  , keyDecoder: jtv.string
                  , decoder: jtv.anyJson
                  }

const query = {value: {value1 : '123'}}
const payload = {value: {value1 : '123', value2: 1}}
const key = "key"

type T={
  value: {value1: string; value2: number};
}

const createdEvent = (cid: ContractId<T>, argument: T = payload): Event<T> => {
  return(
    {created: { templateId: templateId
              , contractId: cid
              , signatories: []
              , observers: []
              , agreementText: ''
              , key: key
              , payload: argument
              }
    }
  )
}

const archivedEvent = (cid: ContractId<T>): Event<T> => {
  return(
    {archived: { templateId: templateId
               , contractId: cid
               }
    }
  )
}

const emptyLedgerStore = () => LedgerStore.setQueryResult(LedgerStore.empty(), template, query, []);

describe('daml-react-hooks', () => {
  registerTemplate(template)

  it("no events result in unchanged state", () => {
    let store = emptyLedgerStore();
    store = LedgerStore.addEvents(store, []);
    expect(store.templateStores.get(template)?.queryResults.get(query)?.contracts).toHaveLength(0);
  });

  it("adding one event", () => {
    let store = emptyLedgerStore();
    store = LedgerStore.addEvents(store, [createdEvent('0#0')]);
    expect(store.templateStores.get(template)?.queryResults.get(query)?.contracts).toHaveLength(1);
  });

  it("adding three events", () => {
    let store = emptyLedgerStore();
    store = LedgerStore.addEvents(store, [createdEvent('0#0'), createdEvent('0#1'), createdEvent('0#2')]);
    expect(store.templateStores.get(template)?.queryResults.get(query)?.contracts).toHaveLength(3);
  });

  it("adding two events and archiving one", () => {
    let store = emptyLedgerStore();
    store = LedgerStore.addEvents(store, [createdEvent('0#0'), createdEvent('0#1'), archivedEvent('0#0')]);
    expect(store.templateStores.get(template)?.queryResults.get(query)?.contracts).toHaveLength(1);
  });

  it("archiving a non-existant contract", () => {
    let store = emptyLedgerStore();
    store = LedgerStore.addEvents(store, [createdEvent('0#0'), archivedEvent('0#2')]);
    expect(store.templateStores.get(template)?.queryResults.get(query)?.contracts).toHaveLength(1);
  });

  it("adding an event that doesn't match the query", () => {
    let store = emptyLedgerStore();
    store = LedgerStore.addEvents(store, [createdEvent('0#0', {value : {value1: 'something else', value2: 1}})]);
    expect(store.templateStores.get(template)?.queryResults.get(query)?.contracts).toHaveLength(0);
  });
});
