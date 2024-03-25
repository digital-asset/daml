# @daml/react

> React framework for Daml applications

<!-- START_BACKLINK -->

## Documentation

Comprehensive documentation for `@daml/react` can be found [here](https://docs.daml.com/0.0.0-SDKVERSION/app-dev/bindings-ts/daml-react/index.html).

<!-- END_BACKLINK -->

## Usage

To get an overview on how to build a Daml application, please read the [application architecture overview](https://docs.daml.com/app-dev/app-arch.html).

To use `@daml/react` your application needs to be connected to the JSON API of a Daml ledger. If
your JSON API server for the ledger runs on the local host on port 7575, set

``` json
"proxy": "http://localhost:7575"
```

in your `package.json` and wrap your main component in the `DamlLedger` component of `@daml/react`

```typescript
import DamlLedger from @daml/react

const App: React.FC = () => {
     <DamlLedger
      token: <your authentication token>
      httpBaseUrl?: <optional http base url>
      wsBaseUrl?: <optional websocket base url>
      reconnectThreshold?: <optional delay in ms>
      party: <the logged in party>
    >
      <MainScreen />
    </DamlLedger>
};
```

Now you can use the following React hooks to interact with a Daml ledger:

`useParty`
----------
`useParty` returns the party, for which commands are currently send to the ledger.

```typescript
const party = useParty();
```

`useLedger`
-------------
`useLedger` returns an instance of the `Ledger` class of [@daml/ledger](https://docs.daml.com/app-dev/bindings-ts/daml-ledger/index.html) to interact with the Daml
ledger.

```typescript
const ledger = useLedger();
const newContract = await ledger.create(ContractTemplate, arguments);
const archiveEvent = await Ledger.archive(ContractTemplate, contractId);
const [choiceReturnValue, events] = await ledger.exercise(ContractChoice, contractId, choiceArguments);
```


`useQuery`
----------
`useQuery` returns the contracts matching a given query. The query matches for a given contract
template and specified field values of the contracts of that template.

```typescript
const {contracts, loading} = useQuery(ContractTemplate, () => {field: value}, [dependency1,
dependency2, ...]);
```

If the query is omitted, all visible contracts of the given template are returned.

```typescript
const {contracts, loading} = useQuery(ContractTemplate);
```

`useReload`
-----------
`useReload` returns a function to reload the results of queries.

```typescript
const reload = useReload();
const onClick = reload;
```

`useStreamQuery`
----------------

> Deprecated: prefer `useStreamQueries`

`useStreamQuery` has the same signature as `useQuery`, but it constantly refreshes the results.

```typescript
const {contracts, loading} = useStreamQuery(ContractTemplate, () => {field: value}, [dependency1,
dependency2, ...]);
```

If the query is omitted, all visible contracts of the given template are returned.

```typescript
const {contracts, loading} = useStreamQuery(ContractTemplate);
```

`useStreamQueries`
------------------

`useStreamQueries` is similar to `useQuery`, except that:
- It constantly refreshes the results.
- The factory function is expected to return a list of queries, and the
  resulting set of contracts is the union of all the contracts that match at
  least one query.
- Like `useQuery`, if no factory function is provided, or if the provided
  function returns an empty array, the set will contain all contracts of that
  template.

```typescript
const {contracts, loading} = useStreamQueries(ContractTemplate,
                                              () => [{field: value}, ...],
                                              [dependency1, dependency2, ...]);
```

You can additionally pass in an extra function to handle WebSocket connection
failures.

`useFetchByKey`
---------------
`useFetchByKey` returns the unique contract of a given template and a given contract key.

```typescript
const {contract, loading} = useFetchByKey(ContractTemplate, () => key, [dependency1, dependency2, ...]);
```

`useStreamFetchByKey`
---------------------

> Deprecated: prefer `useStreamFetchByKeys`

`useStreamFetchByKey` has the same signature as `useFetchByKey`, but it constantly keeps refreshing
the result.

```typescript
const {contract, loading} = useStreamFetchByKey(ContractTemplate, () => key, [dependency1, dependency2, ...]);
```

`useStreamFetchByKeys`
---------------------

`useStreamFetchByKeys` takes a template and a factory that returns a list of
keys, and returns a list of contracts that correspond to those keys (or null if
no contract matches the corresponding key). This hook will keep an open
WebSocket connection and listen for any change to the corresponding contracts.

If the factory function returns an empty array, the hook will similarly produce
an empty array of contracts.


```typescript
const {contracts, loading} = useStreamFetchByKeys(ContractTemplate,
                                                  () => [key1, key2, ...],
                                                  [dependency1, dependency2, ...]);
```

You can additionally pass in an extra function to handle WebSocket connection
failures.

## Advanced Usage

In order to interact as multiple parties or to connect to several ledgers, one needs to create an extra
`DamlLedger` [contexts](https://reactjs.org/docs/context.html) specific to your requirement.

`createLedgerContext`
---------------------
`createLedgerContext` returns another `DamlLedger` context and associated hooks (`useParty`, `useLedger` ... etc)
that will look up their connection within that returned context.

## Source
https://github.com/digital-asset/daml.

## License
[Apache-2.0](License)
