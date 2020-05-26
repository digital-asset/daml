# @daml/react

> React framework for DAML applications

## Documentation

Comprehensive documentation for `@daml/react` can be found [here](https://docs.daml.com/0.0.0-SDKVERSION/app-dev/bindings-ts/daml-react/index.html).

## Usage

The best way to get you started quickly is to look at [Create DAMLApp](https://github.com/digital-asset/create-daml-app)
and to read the [QuickstartGuide](https://docs.daml.com/getting-started/quickstart.html).

To get an overview on how to build a DAML application, please read the [application architecture overview](https://docs.daml.com/app-dev/index.html).

To use `@daml/react` your application needs to be connected to the JSON API of a DAML ledger. If
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
      party: <the logged in party>
    >
      <MainScreen />
    </DamlLedger>
};
```

Now you can use the following React hooks to interact with a DAML ledger:

`useParty`
----------
`useParty` returns the party, for which commands are currently send to the ledger.

```typescript
const party = useParty();
```

`useLedger`
-------------
`useLedger` returns an instance of the `Ledger` class of [@daml/ledger](https://docs.daml.com/app-dev/bindings-ts/daml-ledger/index.html) to interact with the DAML
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
template and specified field values of the contracts of that template. If the query is omitted, all
visible contracts of the given template are returned.

```typescript
const {contracts, loading} = useQuery(ContractTemplate, () => {field: value}, [dependency1,
dependency2, ...]);

const {allContracts, isLoading} = useQuery(ContractTemplate, [dependency1, dependency2, ...]);
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
`useStreamQuery` has the same signature as `useQuery`, but it constantly refreshes the results.

```typescript
const {contracts, loading} = useStreamQuery(ContractTemplate, () => {field: value}, [dependency1,
dependency2, ...]);

const {allContracts, isLoading} = useStreamQuery(ContractTemplate, [dependency1, dependency2, ...]);
```

`useFetchByKey`
---------------
`useFetchByKey` returns the unique contract of a given template and a given contract key.

```typescript
const {contract, loading} = useFetchByKey(ContractTemplate, () => key, [dependency1, dependency2, ...]);
```

`useStreamFetchByKey`
---------------------
`useStreamFetchByKey` has the same signature as `useFetchByKey`, but it constantly keeps refreshes
the result.

```typescript
const {contract, loading} = useStreamFetchByKey(ContractTemplate, () => key, [dependency1, dependency2, ...]);
```


## Source
https://github.com/digital-asset/daml.

## License
[Apache-2.0](License)
