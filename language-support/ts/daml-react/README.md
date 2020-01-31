# @daml/react

> React framework to interact with a DAML ledger.

## Install

### npm:

```sh
npm install @daml/react
```

### yarn:

```sh
yarn add @daml/react
```

## Usage

The best place to get you started is [Create DAML App](https://github.com/digital-asset/create-daml-app)
and the [Quickstart Guide](https://docs.daml.com/getting-started/quickstart.html).

Here is a Typescript/React example:

```typescript
import {useParty, ...} from @daml/react
```

React entry point:

```typescript
const App: React.FC = () => {
  const [credentials, setCredentials] = useState<Credentials | undefined>();

  return credentials
    ? <DamlLedger credentials={credentials}>
        <MainScreen onLogout={() => setCredentials(undefined)}/>
      </DamlLedger>
    : <LoginScreen onLogin={setCredentials}/>;
}
```
DAML Ledger API | Code
----------------|-----
Get the logged in party | `const party = useParty();` <br> `...` <br> `<h1> You're logged in as {party} </h1>`
Exercise a choice on a contract | `const [exerciseChoice] = useExercise(ContractTemplate.ChoiceName)` <br> `...` <br> `onClick={() => exerciseChoice(contractId, arguments)}`
Query the ledger | `const {loading: isLoading, contracts: queryResult} = useQuery(ContractTemplate, () => ({field: value}), [dependency1, dependency2, ... ]) ` 
Query for contract keys | `const contracts = useFetchByKey(ContractTemplate, () => key, [dependency1, dependency2, ...])` 
Reload the query results | `reload = useReload();` <br> `...` <br> `onClick={() => reload()}`

## Source
https://github.com/digital-asset/daml.

## License
[Apache-2.0](License)
