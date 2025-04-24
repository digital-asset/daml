JavaScript/React API
--------------------

Daml ledgers expose a unified API for interaction.

The following describes how to interact with a ledger using the `TypeScript <https://www.typescriptlang.org/>`__ libraries ``@daml/ledger``, ``@daml/react`` in a frontend build with `React <https://reactjs.org/>`__.

Import the libraries via:

.. code-block::

    import Ledger from @daml/ledger
    import {useParty, ...} from @daml/react

React entry point:

.. code-block::

    import DamlLeddger from @daml/react

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

.. list-table::

   * - Get the logged in party
     -
       .. code-block::

            const party = useParty();
            ...
            <h1> You're logged in as {party} </h1>

   * - Query the ledger
     - ``const {contracts: queryResult, loading: isLoading, } = useQuery(ContractTemplate, () => ({field: value}), [dep1, dep2, ...])``
   * - Query for contract keys
     - ``const {contracts, loading} = useFetchByKey(ContractTemplate, () => key, [dep1, dep2, ...])``
   * - Reload the query results
     -
       .. code-block::

            reload = useReload();
            ...
            onClick={() => reload()}

   * - Query the ledger, returns a refreshing stream
     - ``const {contracts, loading} = useStreamQuery(ContractTemplate, () => ({field: value}), [dep1, dep2, …])``
   * - Query for contract keys, returns a refreshing stream
     - ``const {contracts, loading} = useStreamFetchByKey(ContractTemplate, () => key, [dep1, dep2, ...])``
   * - Create a contract on the ledger
     -
       .. code-block::

            const ledger = useLedger();
            const newContract = await ledger.create(ContractTemplate, arguments)

   * - Archive a contract on the ledger
     -
       .. code-block::

            const ledger = useLedger();
            const archiveEvent = await ledger.archive(ContractTemplate, contractId)

   * - Exercise a contract choice on the ledger
     -
       .. code-block::

            const ledger = useLedger();
            const [choiceReturnValue, events] = await ledger.exercise(ContractChoice, contractId, choiceArguments)
