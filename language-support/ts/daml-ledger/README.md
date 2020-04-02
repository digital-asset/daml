# @daml/ledger

> Client side API implementation for a DAML based ledgers. This library implements the [JSON
> API](https://docs.daml.com/json-api/index.html) for a DAML ledger.

## Documentation

Comprehensive documentation for `@daml/ledger` can be found
[here](https://docs.daml.com/0.0.0-SDKVERSION/app-dev/bindings-ts/daml-ledger/index.html).

## Usage

The best way to get you started quickly is to look at [Create DAML
App](https://github.com/digital-asset/create-daml-app) and to read the the [Quickstart
Guide](https://docs.daml.com/getting-started/quickstart.html).

We recommend to use the [React](https://reactjs.org) framework and the `@daml/react` library to
build frontends for DAML applications. If you choose a different Javascript based framework, please
take a look at the source of `@daml/react` and it's usage of the `@daml/ledger` library.

The main export of `@daml/ledger` is the `Ledger` class. It's constructor takes an authentication
token used to communicate with the [JSON API](https://docs.daml.com/json-api/index.html), an HTTP
base URL and a websocket base URL.

An instance of the `Ledger` class provides the following methods to communicate with a DAML ledger.
Please consult the [documentation](https://docs.daml.com/app-dev/bindings-ts/daml-ledger/index.html)
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

`query`
-------
Retrieve contracts for a given template matching a given query. If no query is given, all contracts
of that template visible for the submitting party are returned.

`streamQuery`
-------------
Retrieve a consolidated stream of events for a given template and query. The accumulated state is
the current set of active contracts matching the query. An event can be a `CreateEvent` or an
`ArchiveEvent`. When no `query` argument is given, all events visible to the submitting party are
returned.

`fetch`
-------
Fetch a contract identified by its contract id.

`fetchByKey`
------------
Fetch a contract identified by its contract key.

`streamFetchByKey`
------------------
Retrieve a consolidated stream of `CreateEvent`'s for a given template and contract key.


## Source
https://github.com/digital-asset/daml/tree/master/language-support/ts/daml-ledger

## License
[Apache-2.0](https://github.com/digital-asset/daml/blob/master/LICENSE)
