# @daml/ledger

> Client side API implementation for a DAML based ledgers. This library implements the [JSON
> API](https://docs.daml.com/json-api/index.html) for a DAML ledger.

## Documentation

Comprehensive documentation for `@daml/ledger` can be found
[here](https://docs.daml.com/0.0.0-SDKVERSION/app-dev/bindings-ts/daml-ledger/index.html).

## Usage

The best way to get you started quickly is to look at [Create DAML
App](https://github.com/digital-asset/create-daml-app) and to read the [Quickstart
Guide](https://docs.daml.com/getting-started/quickstart.html).

We recommend to use the [React](https://reactjs.org) framework and the `@daml/react` library to
build frontends for DAML applications. If you choose a different Javascript based framework, please
take a look at the source of `@daml/react` and it's usage of the `@daml/ledger` library.

The main export of `@daml/ledger` is the `Ledger` class. It's constructor takes an authentication
token used to communicate with the [JSON API](https://docs.daml.com/json-api/index.html), an HTTP
base URL and a websocket base URL.

An instance of the `Ledger` class provides the following methods to communicate with a DAML ledger.
Please consult the
[documentation](https://docs.daml.com/app-dev/bindings-ts/daml-ledger/classes/_index_.ledger.html)
for their exact signatures.

`create`
--------
Create a new contract of the given template with given arguments.

`archive`
---------
Archive a contract identified by its contract id.

`archiveByKey`
--------------
Archive a contract identified by its contract key.

`exercise`
----------
Exercise a choice on a contract identified by its contract id.

`exerciseByKey`
---------------
Exercise a choice on a contract identified by its contract key.

`createAndExercise`
-------------------

Create a new contract and, within the same transaction, immediately exercise a
choice on it. Primarily meant for consuming choices, but that's not a
requirement.

`query`
-------
Retrieve contracts for a given template matching a given query. If no query is given, all contracts
of that template visible for the submitting party are returned.

`streamQuery`
-------------

> Deprecated: prefer `streamQueries`.

Retrieve a consolidated stream of events for a given template and optional
query. The accumulated state is the current set of active contracts matching
the query if one was given; if the function was called without a query
argument, or the query argument was `undefined`, the accumulated state will
instead contain all of the active contracts for the given template.

`streamQueries`
---------------
Retrieve a consolidated stream of events for a given template and queries. The
accumulated state is the current set of active contracts matching at least one
of the given queries, or all contracts for the given template if no query is
given.

`fetch`
-------
Fetch a contract identified by its contract id.

`fetchByKey`
------------
Fetch a contract identified by its contract key.

`streamFetchByKey`
------------------

> Deprecated: prefer `streamFetchByKeys`.

Retrieve a consolidated stream of `CreateEvent`'s for a given template and
contract key. The accumulated state is either the `CreateEvent` for the active
contract matching the given key, or null if there is no currently-active
contract for the given key.

`streamFetchByKeys`
-------------------
Retrieve a consolidated stream of `CreateEvent`'s for a given template and
contract keys. The accumulated state is a vector of the same length as the
given vector of keys, where each element is the CreateEvent for the current
active contract of the corresponding key (element-wise), or null if there is no
current active contract for that key.

Note: the given `key` objects will be compared for (deep) equality with the
values returned by the API. As such, they have to be given in the "output"
format of the JSON API. See the [JSON API docs] for details.

[JSON API docs]: https://docs.daml.com/json-api/lf-value-specification.html

`getParties`
------------

For a given list of party identifiers, returns full information, or null if
the party doesn't exist.

`listKnownParties`
------------------

Returns an array of PartyInfo for all parties on the ledger.

`allocateParty`
---------------

Allocates a new party.

## Source

https://github.com/digital-asset/daml/tree/master/language-support/ts/daml-ledger

## License

[Apache-2.0](https://github.com/digital-asset/daml/blob/master/LICENSE)
