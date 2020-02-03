// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Party, Int, Template, ContractId, Date, registerTemplate } from '@daml/types';
import { CreateEvent, Query, ArchiveEvent } from '@daml/ledger';
import * as jtv from '@mojotech/json-type-validation';
import * as Store from './templateStore';

// Mock template
type Foo = {
  owner: Party;
  count: Int;
  date: Date;
}
// eslint-disable-next-line @typescript-eslint/no-namespace
namespace Foo {
  export type Key = {
    _1: Party;
    _2: Int;
  }
}
const Foo: Template<Foo, Foo.Key, 'Mock:Foo'> = {
  templateId: 'Mock:Foo',
  keyDecoder: undefined as unknown as () => jtv.Decoder<Foo.Key>,
  decoder: undefined as unknown as () => jtv.Decoder<Foo>,
  Archive: {
    template: () => Foo,
    choiceName: 'Archive',
    argumentDecoder: undefined as unknown as () => jtv.Decoder<{}>,
    resultDecoder: undefined as unknown as () => jtv.Decoder<{}>,
  },
};
registerTemplate(Foo);

let nextContractId: number = 1;
function makeContractId(): ContractId<Foo> {
  const contractId = `#${nextContractId}`;
  nextContractId += 1;
  return contractId;
}

function makeCreateEvent(foo: Foo): CreateEvent<Foo, Foo.Key> {
  return {
    templateId: Foo.templateId,
    contractId: makeContractId(),
    signatories: [foo.owner],
    observers: [],
    agreementText: '',
    key: {_1: foo.owner, _2: foo.count},
    payload: foo,

  };
}

function makeArchiveEvent(contract: CreateEvent<Foo, Foo.Key>): {archived: ArchiveEvent<Foo>} {
  return {archived: {templateId: contract.templateId, contractId: contract.contractId}};
}

function emptyFooStore(): Store.Store<Foo, Foo.Key> {
  return Store.empty();
}

const alice1 = makeCreateEvent({owner: 'Alice', count: '1', date: '2020-02-02'});
const alice2 = makeCreateEvent({owner: 'Alice', count: '2', date: '2020-02-02'});
const bob1 = makeCreateEvent({owner: 'Bob', count: '1', date: '2020-02-02'});
const bob2 = makeCreateEvent({owner: 'Bob', count: '2', date: '2020-02-02'});
const alice1Next = makeCreateEvent({owner: 'Alice', count: '1', date: '2020-02-03'});

const emptyQuery: Query<Foo> = {};
const aliceQuery: Query<Foo> = {owner: 'Alice'};
const bobQuery: Query<Foo> = {owner: 'Bob'};
const oneQuery: Query<Foo> = {count: '1'};
const twoQuery: Query<Foo> = {count: '2'};

const alice1Key: Foo.Key = {_1: 'Alice', _2: '1'};
const bob1Key: Foo.Key = {_1: 'Bob', _2: '1'};
// NOTE(MH): We need a version of `alice1Key` which has the same value but a
// different reference.
const alice1KeyAlt: Foo.Key = {_1: 'Alice', _2: '1'};

describe('setQueryResults', () => {
  it('set empty result', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([]);
  });

  it('set non-empty result', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('reset result', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.setQueryResult(store, aliceQuery, [alice2]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice2]);
  });

  it('set multiple results', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.setQueryResult(store, bobQuery, [bob1]);
    expect(store.queryResults.size).toBe(2);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
    expect(store.queryResults.get(bobQuery)?.contracts).toEqual([bob1]);
  });

  it('reset multiple results', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.setQueryResult(store, bobQuery, [bob1]);
    store = Store.setQueryResult(store, aliceQuery, [alice2]);
    expect(store.queryResults.size).toBe(2);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice2]);
    expect(store.queryResults.get(bobQuery)?.contracts).toEqual([bob1]);
  });
});

describe('addEvents/queryResults', () => {
  it('no events and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, []);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([]);
  });

  it('no events and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, []);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('single relevant create event and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [{created: alice1}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('multiple relevant create events and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [{created: alice1}, {created: alice2}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1, alice2]);
  });

  it('relevant and irrelevant create events and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [{created: alice1}, {created: bob1}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('single relevant create event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, [{created: alice2}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1, alice2]);
  });

  it('relevant and irrelevant create events and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, [{created: alice2}, {created: bob1}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1, alice2]);
  });

  it('create event matching multiple queries', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, emptyQuery, []);
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.setQueryResult(store, bobQuery, []);
    store = Store.setQueryResult(store, oneQuery, []);
    store = Store.setQueryResult(store, twoQuery, []);
    store = Store.addEvents(store, [{created: alice1}]);
    expect(store.queryResults.size).toBe(5);
    expect(store.queryResults.get(emptyQuery)?.contracts).toEqual([alice1]);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
    expect(store.queryResults.get(bobQuery)?.contracts).toEqual([]);
    expect(store.queryResults.get(oneQuery)?.contracts).toEqual([alice1]);
    expect(store.queryResults.get(twoQuery)?.contracts).toEqual([]);
  });

  it('multiple create events matching multiple queries', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, emptyQuery, []);
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.setQueryResult(store, bobQuery, []);
    store = Store.setQueryResult(store, oneQuery, []);
    store = Store.setQueryResult(store, twoQuery, []);
    store = Store.addEvents(store, [{created: alice1}, {created: alice2}, {created: bob1}]);
    expect(store.queryResults.size).toBe(5);
    expect(store.queryResults.get(emptyQuery)?.contracts).toEqual([alice1, alice2, bob1]);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1, alice2]);
    expect(store.queryResults.get(bobQuery)?.contracts).toEqual([bob1]);
    expect(store.queryResults.get(oneQuery)?.contracts).toEqual([alice1, bob1]);
    expect(store.queryResults.get(twoQuery)?.contracts).toEqual([alice2]);
  });

  it('archive event and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [makeArchiveEvent(alice1)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([]);
  });

  it('relevant archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1, alice2]);
    store = Store.addEvents(store, [makeArchiveEvent(alice1)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice2]);
  });

  it('irrelevant archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, [makeArchiveEvent(alice2)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('relevant and irrelevant archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, [makeArchiveEvent(alice1), makeArchiveEvent(bob1)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([]);
  });

  it('archive event matching multiple queries', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1, alice2]);
    store = Store.setQueryResult(store, bobQuery, [bob1]);
    store = Store.setQueryResult(store, oneQuery, [alice1, bob1]);
    store = Store.addEvents(store, [makeArchiveEvent(alice1)]);
    expect(store.queryResults.size).toBe(3);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice2]);
    expect(store.queryResults.get(bobQuery)?.contracts).toEqual([bob1]);
    expect(store.queryResults.get(oneQuery)?.contracts).toEqual([bob1]);
  });

  it('create and archive event and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [{created: alice1}, makeArchiveEvent(alice2)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('create and archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, [{created: alice2}, makeArchiveEvent(alice1)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice2]);
  });

  it('archive and create event and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [makeArchiveEvent(alice2), {created: alice1}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1]);
  });

  it('archive and create event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.addEvents(store, [makeArchiveEvent(alice1), {created: alice2}]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice2]);
  });

  it('create and archive event for same and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    store = Store.addEvents(store, [{created: alice1}, makeArchiveEvent(alice1)]);
    expect(store.queryResults.size).toBe(1);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([]);
  });

  // NOTE(MH): The JSON API does not do this to us.
  it('create and archive event for same in bad order and empty store', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, aliceQuery, []);
    expect(() => Store.addEvents(store, [makeArchiveEvent(alice1), {created: alice1}]))
      .toThrow('bad order of create and archive events');
  });

  it('create and archive event matching multiple queries', () => {
    let store = emptyFooStore();
    store = Store.setQueryResult(store, emptyQuery, [alice1, bob1, bob2]);
    store = Store.setQueryResult(store, aliceQuery, [alice1]);
    store = Store.setQueryResult(store, bobQuery, [bob1, bob2]);
    store = Store.setQueryResult(store, oneQuery, [alice1, bob1]);
    store = Store.setQueryResult(store, twoQuery, [bob2]);
    store = Store.addEvents(store, [makeArchiveEvent(bob2), {created: alice2}]);
    expect(store.queryResults.size).toBe(5);
    expect(store.queryResults.get(emptyQuery)?.contracts).toEqual([alice1, bob1, alice2]);
    expect(store.queryResults.get(aliceQuery)?.contracts).toEqual([alice1, alice2]);
    expect(store.queryResults.get(bobQuery)?.contracts).toEqual([bob1]);
    expect(store.queryResults.get(oneQuery)?.contracts).toEqual([alice1, bob1]);
    expect(store.queryResults.get(twoQuery)?.contracts).toEqual([alice2]);
  });
});

describe('setFetchByKeyResults', () => {
  it('set null', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
    expect(store.fetchByKeyResults.get(alice1KeyAlt)).toBeUndefined();
  });

  it('set contract', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(alice1KeyAlt)).toBeUndefined();
  });

  it('reset contract to null', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('reset null to contract', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
  });

  it('reset contract to contract', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.setFetchByKeyResult(store, alice1Key, alice1Next);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });

  it('set multiple with different value', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.setFetchByKeyResult(store, bob1Key, bob1);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(bob1Key)?.contract).toEqual(bob1);
  });

  it('set multiple with same value and different reference', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.setFetchByKeyResult(store, alice1KeyAlt, alice1Next);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(alice1KeyAlt)?.contract).toEqual(alice1Next);
  });
});

describe('addEvents/fetchByKeyResults', () => {
  it('no events and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, []);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('no events and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, []);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
  });

  it('single relevant create event and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [{created: alice1}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
  });

  it('single relevant create event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [{created: alice1Next}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });

  it('irrelevant create events and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [{created: alice2}, {created: bob1}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('irrelevant create events and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [{created: alice1}, {created: bob1}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
  });

  it('relevant and irrelevant create events and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [{created: alice1}, {created: alice2}, {created: bob1}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
  });

  it('relevant and irrelevant create events and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [{created: alice1Next}, {created: alice2}, {created: bob1}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });

  it('single create event matching multiple keys', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.setFetchByKeyResult(store, alice1KeyAlt, null);
    store = Store.addEvents(store, [{created: alice1}]);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(alice1KeyAlt)?.contract).toEqual(alice1);
  });

  it('create events matching multiple keys', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.setFetchByKeyResult(store, bob1Key, null);
    store = Store.addEvents(store, [{created: alice1}, {created: bob1}]);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(bob1Key)?.contract).toEqual(bob1);
  });

  it('archive event and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [makeArchiveEvent(alice1)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('relevant archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [makeArchiveEvent(alice1)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('irrelevant archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [makeArchiveEvent(alice2)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
  });

  it('relevant and irrelevant archive event and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [makeArchiveEvent(alice1), makeArchiveEvent(alice2)]);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('archive event matching multiple keys', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.setFetchByKeyResult(store, alice1KeyAlt, alice1);
    store = Store.addEvents(store, [makeArchiveEvent(alice1)]);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
    expect(store.fetchByKeyResults.get(alice1KeyAlt)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1KeyAlt)?.contract).toBeNull();
  });

  it('create and archive event for same contract and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [{created: alice1}, makeArchiveEvent(alice1)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('create and archive event for different keys and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.setFetchByKeyResult(store, bob1Key, bob1);
    store = Store.addEvents(store, [{created: alice1}, makeArchiveEvent(bob1)]);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(bob1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(bob1Key)?.contract).toBeNull();
  });

  it('archive and create event for same key and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [makeArchiveEvent(alice1), {created: alice1Next}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });

  it('archive and create event for same contract in bad order and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    expect(() => Store.addEvents(store, [makeArchiveEvent(alice1), {created: alice1}]))
      .toThrow('bad order of create and archive events');
  });

  it('archive and create event for different keys and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.setFetchByKeyResult(store, bob1Key, bob1);
    store = Store.addEvents(store, [makeArchiveEvent(bob1), {created: alice1}]);
    expect(store.fetchByKeyResults.size).toBe(2);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1);
    expect(store.fetchByKeyResults.get(bob1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(bob1Key)?.contract).toBeNull();
  });

  it('create and archive event for same key in bad order and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [{created: alice1Next}, makeArchiveEvent(alice1)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });

  it('create and archive and create event for same key and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [{created: alice1}, makeArchiveEvent(alice1), {created: alice1Next}]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });

  it('archive and create and archive event for same key and populated store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, alice1);
    store = Store.addEvents(store, [makeArchiveEvent(alice1), {created: alice1Next}, makeArchiveEvent(alice1Next)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)).toBeDefined();
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toBeNull();
  });

  it('create and create and archive event for same key in bad order and empty store', () => {
    let store = emptyFooStore();
    store = Store.setFetchByKeyResult(store, alice1Key, null);
    store = Store.addEvents(store, [{created: alice1}, {created: alice1Next}, makeArchiveEvent(alice1)]);
    expect(store.fetchByKeyResults.size).toBe(1);
    expect(store.fetchByKeyResults.get(alice1Key)?.contract).toEqual(alice1Next);
  });
});
