.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Extractor
#########

Introduction
************

You can use the Extractor to extract contract data for a single party from a Ledger node into a PostgreSQL database.

It is useful for:

- **Application developers** to access data on the ledger, observe the evolution of data, and debug their applications
- **Business analysts** to analyze ledger data and create reports
- **Support teams** to debug any problems that happen in production

Using the Extractor, you can:

- Take a full snapshot of the ledger (from the start of the ledger to the current latest transaction)
- Take a partial snapshot of the ledger (between specific `offsets <../../app-dev/grpc/proto-docs.html#ledgeroffset>`__)
- Extract historical data and then stream indefinitely (either from the start of the ledger or from a specific offset)

Setting up
**********

Prerequisites:

- A PostgreSQL database that is reachable from the machine the Extractor runs on. Use PostgreSQL version 9.4 or later to have JSONB type support that is used in the Extractor.
- We recommend using an empty database to avoid schema and table collisions. To see which tables to expect, see  `Output format <#output-format>`__.
- A running Sandbox or Ledger Node as the source of data.
- You’ve :doc:`installed the SDK </getting-started/installation>`.

Once you have the prerequisites, you can start the Extractor like this::

$ daml extractor --help

Trying it out
*************

This example extracts: 

- all contract data from the beginning of the ledger to the current latest transaction
- for the party ``Scrooge_McDuck``
- from a Ledger node or Sandbox running on host ``192.168.1.12`` on port ``6865``
- to PostgreSQL instance running on localhost
- identified by the user ``postgres`` without a password set
- into a database called ``daml_export``

  .. code-block:: none

    $ daml extractor postgresql --user postgres --connecturl jdbc:postgresql:daml_export --party Scrooge_McDuck -h 192.168.1.12 -p 6865 --to head

This terminates after reaching the transaction which was the latest at the time the Extractor started streaming. 

To run the Extractor indefinitely, and thus keeping the database up to date as new transactions arrive on the ledger, omit the ``--to head`` parameter to fall back to the default streaming-indefinitely approach, or state explicitly by using the ``--to follow`` parameter.

Running the Extractor
*********************

The basic command to run the Extractor is::

  $ daml extractor [options]

For what options to use, see the next sections.

Connecting the Extractor to a ledger
************************************

To connect to the Sandbox, provide separate address and port parameters. For example, ``--host 10.1.1.10 --port 6865``, or in short form ``-h 10.1.1.168 -p 6865``.

The default host is ``localhost`` and the default port is ``6865``, so you don’t need to pass those.

To connect to a Ledger node, you might have to provide SSL certificates. The options for doing this are shown in the output of the ``--help`` command.

Connecting to your database
***************************

As usual for a Java application, the database connection is handled by the well-known JDBC API, so you need to provide:

- a JDBC connection URL
- a username
- an optional password

For more on the connection URL, visit https://jdbc.postgresql.org/documentation/80/connect.html.

This example connects to a PostgreSQL instance running on ``localhost`` on the default port, with a user ``postgres`` which does not have a password set, and a database called ``daml_export``. This is a typical setup on a developer machine with a default PostgreSQL install

.. code-block:: none

  $ daml extractor postgres --connecturl jdbc:postgresql:daml_export --user postgres --party [party]

This example connects to a database on host ``192.168.1.12``, listening on port ``5432``. The database is called ``daml_export``, and the user and password used for authentication are ``daml_exporter`` and ``ExamplePassword``

.. code-block:: none

  $ daml extractor postgres --connecturl jdbc:postgresql://192.168.1.12:5432/daml_export --user daml_exporter --password ExamplePassword --party [party]

Authenticating Extractor
************************

If you are running Extractor against a Ledger API server that requires authentication, you must provide the access token when you start it.

The access token retrieval depends on the specific DAML setup you are working with: please refer to the ledger operator to learn how.

Once you have retrieved your access token, you can provide it to Extractor by storing it in a file and provide the path to it using the ``--access-token-file`` command line option.

Both in the case in which the token cannot be read from the provided path or if the Ledger API reports an authentication error (for example due to token expiration), Extractor will keep trying to read and use it and report the error via logging. This retry mechanism allows expired token to be overwritten with valid ones and keep Extractor going from where it left off.

Full list of options
********************

To see the full list of options, run the ``--help`` command, which gives the following output:

.. code-block:: none

  Usage: extractor [prettyprint|postgresql] [options]

  Command: prettyprint [options]
  Pretty print contract template and transaction data to stdout.
    --width <value>          How wide to allow a pretty-printed value to become before wrapping.
                             Optional, default is 120.
    --height <value>         How tall to allow each pretty-printed output to become before
                             it is truncated with a `...`.
                             Optional, default is 1000.

  Command: postgresql [options]
  Extract data into a PostgreSQL database.
    --connecturl <value>     Connection url for the `org.postgresql.Driver` driver. For examples,
                             visit https://jdbc.postgresql.org/documentation/80/connect.html
    --user <value>           The database user on whose behalf the connection is being made.
    --password <value>       The user's password. Optional.

  Common options:
    -h, --ledger-host <h>    The address of the Ledger host. Default is 127.0.0.1
    -p, --ledger-port <p>    The port of the Ledger host. Default is 6865.
    --ledger-api-inbound-message-size-max <bytes>
                             Maximum message size from the ledger API. Default is 52428800 (50MiB).
    --party <value>          The party or parties whose contract data should be extracted.
                           Specify multiple parties separated by a comma, e.g. Foo,Bar
    -t, --templates <module1>:<entity1>,<module2>:<entity2>...
                             The list of templates to subscribe for. Optional, defaults to all ledger templates.
    --from <value>           The transaction offset (exclusive) for the snapshot start position.
                             Must not be greater than the current latest transaction offset.
                             Optional, defaults to the beginning of the ledger.
                             Currently, only the integer-based Sandbox offsets are supported.
    --to <value>             The transaction offset (inclusive) for the snapshot end position.
                             Use “head” to use the latest transaction offset at the time
                             the extraction first started, or “follow” to stream indefinitely.
                             Must not be greater than the current latest offset.
                             Optional, defaults to “follow”.
    --help                   Prints this usage text.

  TLS configuration:
    --pem <value>            TLS: The pem file to be used as the private key.
    --crt <value>            TLS: The crt file to be used as the cert chain.
                             Required if any other TLS parameters are set.
    --cacrt <value>          TLS: The crt file to be used as the the trusted root CA.

  Authentication:
    --access-token-file <value>
                             provide the path from which the access token will be read, required to interact with an authenticated ledger, no default

Some options are tied to a specific subcommand, like ``--connecturl`` only makes sense for the ``postgresql``, while others are general, like ``--party``.

Output format
*************

To understand the format that Extractor outputs into a PostgreSQL database, you need to understand how the ledger stores data.

The DAML Ledger is composed of transactions, which contain events. Events can represent:

- creation of contracts (“create event”), or
- exercise of a choice on a contract (“exercise event”).

A contract on the ledger is either active (created, but not yet archived), or archived. The relationships between transactions and contracts are captured in the database: all contracts have pointers (foreign keys) to the transaction in which they were created, and archived contracts have pointers to the transaction in which they were archived.

Transactions
************

Transactions are stored in the ``transaction table`` in the ``public`` schema, with the following structure

.. code-block:: none

  CREATE TABLE transaction
    (transaction_id TEXT PRIMARY KEY NOT NULL
    ,seq BIGSERIAL UNIQUE NOT NULL
    ,workflow_id TEXT
    ,effective_at TIMESTAMP NOT NULL
    ,extracted_at TIMESTAMP DEFAULT NOW()
    ,ledger_offset TEXT NOT NULL
    );

- **transaction_id**: The transaction ID, as appears on the ledger. This is the primary key of the table.
- **transaction_id**, **effective_at, workflow_id, ledger_offset**: These columns are the properties of the transaction on the ledger. For more information, see the `specification <../../app-dev/grpc/proto-docs.html#transactiontree>`__.
- **seq**: Transaction IDs should be treated as arbitrary text values: you can’t rely on them for ordering transactions in the database. However, transactions appear on the Ledger API transaction stream in the same order as they were accepted on the ledger. You can use this to work around the arbitrary nature of the transaction IDs, which is the purpose of the ``seq`` field: it gives you a total ordering of the transactions, as they happened from the perspective of the ledger. Be aware that ``seq`` is not the exact index of the given transaction on the ledger. Due to the privacy model of DAML Ledgers, the transaction stream won’t deliver a transaction which doesn’t concern the party which is subscribed. The transaction with ``seq`` of 100 might be the 1000th transaction on the ledger; in the other 900, the transactions contained only events which mustn’t be seen by you.
- **extracted_at**: The ``extracted_at`` field means the date the transaction row and its events were inserted into the database. When extracting historical data, this field will point to a possibly much later time than ``effective_at``.

Contracts
*********

Create events and contracts that are created in those events are stored in the ``contract`` table in the ``public`` schema, with the following structure

.. code-block:: none

  CREATE TABLE contract
    (event_id TEXT PRIMARY KEY NOT NULL
    ,archived_by_event_id TEXT DEFAULT NULL
    ,contract_id TEXT NOT NULL
    ,transaction_id TEXT NOT NULL
    ,archived_by_transaction_id TEXT DEFAULT NULL
    ,is_root_event BOOLEAN NOT NULL
    ,package_id TEXT NOT NULL
    ,template TEXT NOT NULL
    ,create_arguments JSONB NOT NULL
    ,witness_parties JSONB NOT NULL
    );

- **event_id, contract_id, create_arguments, witness_parties**: These fields are the properties of the corresponding ``CreatedEvent`` class in a transaction. For more information, see the `specification <../../app-dev/grpc/proto-docs.html#createdevent>`__.
- **package_id, template**: The fields ``package_id`` and ``template`` are the exploded version of the ``template_id`` property of the ledger event.
- **transaction_id**: The ``transaction_id`` field refers to the transaction in which the contract was created.
- **archived_by_event_id, archived_by_transaction_id**: These fields will contain the event id and the transaction id in which the contract was archived once the archival happens.
- **is_root_event**: Indicates whether the event in which the contract was created was a root event of the corresponding transaction.

Every contract is placed into the same table, with the contract parameters put into a single column in a JSON-encoded format. This is similar to what you would expect from a document store, like MongoDB. For more information on the JSON format, see the `later section <#json-format>`__.

Exercises
*********

Exercise events are stored in the ``exercise`` table in the ``public`` schema, with the following structure::

  CREATE TABLE
    exercise
    (event_id TEXT PRIMARY KEY NOT NULL
    ,transaction_id TEXT NOT NULL
    ,is_root_event BOOLEAN NOT NULL
    ,contract_id TEXT NOT NULL
    ,package_id TEXT NOT NULL
    ,template TEXT NOT NULL
    ,contract_creating_event_id TEXT NOT NULL
    ,choice TEXT NOT NULL
    ,choice_argument JSONB NOT NULL
    ,acting_parties JSONB NOT NULL
    ,consuming BOOLEAN NOT NULL
    ,witness_parties JSONB NOT NULL
    ,child_event_ids JSONB NOT NULL
    );

- **package_id, template**: The fields ``package_id`` and ``template`` are the exploded version of the ``template_id`` property of the ledger event.
- **is_root_event**: Indicates whether the event in which the contract was created was a root event of the corresponding transaction.
- **transaction_id**: The ``transaction_id`` field refers to the transaction in which the contract was created.
- The other columns are properties of the ``ExercisedEvent`` class in a transaction. For more information, see the `specification <../../app-dev/grpc/proto-docs.html#exercisedevent>`__.

JSON format
***********

Values on the ledger can be either primitive types, user-defined ``records``, or ``variants``. An extracted contract is represented in the database as a ``record`` of its create argument, and the fields of that ``records`` are either primitive types, other ``records``, or ``variants``. A contract can be a recursive structure of arbitrary depth.

These types are translated to `JSON types <https://json-schema.org/understanding-json-schema/reference/index.html>`_ the following way:

**Primitive types**

- ``ContractID``: represented as `string <https://json-schema.org/understanding-json-schema/reference/string.html>`_.
- ``Int64``: represented as `string <https://json-schema.org/understanding-json-schema/reference/string.html>`_.
- ``Decimal``: A decimal value with precision 38 (38 decimal digits), of which 10 after the comma / period. Represented as `string <https://json-schema.org/understanding-json-schema/reference/string.html>`_.
- ``List``: represented as `array <https://json-schema.org/understanding-json-schema/reference/array.html>`_.
- ``Text``: represented as `string <https://json-schema.org/understanding-json-schema/reference/string.html>`_.
- ``Date``: days since the unix epoch. represented as `integer <https://json-schema.org/understanding-json-schema/reference/numeric.html#integer>`_.
- ``Time``: Microseconds since the UNIX epoch. Represented as `number <https://json-schema.org/understanding-json-schema/reference/numeric.html#number>`_.
- ``Bool``: represented as `boolean <https://json-schema.org/understanding-json-schema/reference/boolean.html>`_.
- ``Party``: represented as `string <https://json-schema.org/understanding-json-schema/reference/string.html>`_.
- ``Unit`` and ``Empty`` are represented as empty records.
- ``Optional``: represented as `object <https://json-schema.org/understanding-json-schema/reference/object.html>`_, as it was a ``Variant`` with two possible constructors: ``None`` and ``Some``.

**User-defined types**

- ``Record``: represented as `object <https://json-schema.org/understanding-json-schema/reference/object.html>`_, where each create parameter’s name is a key, and the parameter’s value is the JSON-encoded value.
- ``Variant``: represented as `object <https://json-schema.org/understanding-json-schema/reference/object.html>`_, using the ``{constructor: body}`` format, e.g. ``{"Left": true}``.

Examples of output
******************

The following examples show you what output you should expect. The Sandbox has already run the scenarios of a DAML model that created two transactions: one creating a ``Main:RightOfUseOffer`` and one accepting it, thus archiving the original contract and creating a new ``Main:RightOfUseAgreement`` contract. We also added a new offer manually.

This is how the ``transaction`` table looks after extracting data from the ledger:

.. figure:: images/transactions.png
   :align: center

You can see that the transactions which were part of the scenarios have the format ``scenario-transaction-{n}``, while the transaction created manually is a simple number. This is why the ``seq`` field is needed for ordering. In this output, the ``ledger_offset`` field has the same values as the ``seq`` field, but you should expect similarly arbitrary values there as for transaction IDs, so better rely on the ``seq`` field for ordering.

This is how the ``contract`` table looks:

.. figure:: images/contracts.png
   :align: center

You can see that the ``archived_by_transacion_id`` and ``archived_by_event_id`` fields of contract ``#0:0`` is not empty, thus this contract is archived. These fields of contracts ``#1:1`` and ``#2:0`` are ``NULL`` s, which mean they are active contracts, not yet archived.

This is how the ``exercise`` table looks:

.. figure:: images/exercises.png
   :align: center

You can see that there was one exercise ``Accept`` on contract ``#0:0``, which was the consuming choice mentioned above.


Dealing with schema evolution
*****************************

When updating packages, you can end up with multiple versions of the same package in the system.

Let’s say you have a template called ``My.Company.Finance.Account``::

  daml 1.2 module My.Company.Finance.Account where
 
  template Account
    with
      provider: Party
      accountId: Text
      owner: Party
      observers: [Party]
    where
      [...]

This is built into a package with a resulting hash ``6021727fe0822d688ddd545997476d530023b222d02f1919567bd82b205a5ce3``.

Later you add a new field, ``displayName``::

  daml 1.2 module My.Company.Finance.Account where
 
  template Account
    with
      provider: Party
      accountId: Text
      owner: Party
      observers: [Party]
      displayName: Text
    where
      [...]

The hash of the new package with the update is ``1239d1c5df140425f01a5112325d2e4edf2b7ace223f8c1d2ebebe76a8ececfe``.

There are contract instances of first version of the template which were created before the new field is added, and there are contract instances of the new version which were created since. Let’s say you have one instance of each::

  {  
    "owner":"Bob",
    "provider":"Bob",
    "accountId":"6021-5678",
    "observers":[  
        "Alice"
    ]
  }

and::

  {  
    "owner":"Bob",
    "provider":"Bob",
    "accountId":"1239-4321",
    "observers":[  
        "Alice"
    ],
    "displayName":"Personal"
  }

They will look like this when extracted:

.. figure:: images/extracted.png
   :align: center

To have a consistent view of the two versions with a default value ``NULL`` for the missing field of instances of older versions, you can create a view which contains all ``Account`` rows::

  CREATE VIEW account_view AS
  SELECT 
     create_arguments->>'owner' AS owner
    ,create_arguments->>'provider' AS provider
    ,create_arguments->>'accountId' AS accountId
    ,create_arguments->>'displayName' AS displayName
    ,create_arguments->'observers' AS observers
  FROM
    contract
  WHERE
    package_id = '1239d1c5df140425f01a5112325d2e4edf2b7ace223f8c1d2ebebe76a8ececfe'
    AND
    template = 'My.Company.Finance.Account'
  UNION
  SELECT 
     create_arguments->>'owner' AS owner
    ,create_arguments->>'provider' AS provider
    ,create_arguments->>'accountId' AS accountId
    ,NULL as displayName
    ,create_arguments->'observers' AS observers
  FROM
    contract
  WHERE
    package_id = '6021727fe0822d688ddd545997476d530023b222d02f1919567bd82b205a5ce3'
    AND
    template = 'My.Company.Finance.Account';

Then, ``account_view will`` contain both contract instances:

.. figure:: images/account.png
   :align: center

Logging
*******

By default, the Extractor logs to stderr, with INFO verbose level. To change the level, use the ``-DLOGLEVEL=[level]`` option, e.g. ``-DLOGLEVEL=TRACE``. 

You can supply your own logback configuration file via the standard method: https://logback.qos.ch/manual/configuration.html

Continuity
**********

When you terminate the Extractor and restart it, it will continue from where it left off. This happens because, when running, it saves its state into the ``state`` table in the ``public`` schema of the database. When started, it reads the contents of this table. If there’s a saved state from a previous run, it restarts from where it left off. There’s no need to explicitly specify anything, this is done automatically. 

DO NOT modify content of the ``state`` table. Doing so can result in the Extractor not being able to continue running against the database. If that happens, you must delete all data from the database and start again.

If you try to restart the Extractor against the same database but with different configuration, you will get an error message indicating which parameter is incompatible with the already exported data. This happens when the settings are incompatible: for example, if previously contract data for the party ``Alice`` was extracted, and now you want to extract for the party ``Bob``.

The only parameters that you can change between two sessions running against the same database are the connection parameters to both the ledger and the database. Both could have moved to different addresses, and the fact that it’s still the same Ledger will be validated by using the Ledger ID (which is saved when the Extractor started its work the first time).

Fault tolerance
***************

Once the Extractor connects to the Ledger Node and the database and creates the table structure from the fetched DAML packages, it wraps the transaction stream in a restart logic with an exponential backoff. This results in the Extractor not terminating even when the transaction stream is aborted for some reason (the ledger node is down, there’s a network partition, etc.). 

Once the connection is back, it continues the stream from where it left off. If it can’t reach the node on the host/port pair the Extractor was started with, you need to manually stop it and restart with the updated address.

Transactions on the ledger are inserted into PostgreSQL as atomic SQL transactions. This means either the whole transaction is inserted or nothing, so you can’t end up with inconsistent data in the database.

Troubleshooting
***************

Can’t connect to the Ledger Node
================================
  
If the Extractor can’t connect to the Ledger node on startup, you’ll see a message like this in the logs, and the Extractor will terminate::

  16:47:51.208 ERROR c.d.e.Main$@[akka.actor.default-dispatcher-7] - FAILURE:
  io.grpc.StatusRuntimeException: UNAVAILABLE: io exception.
  Exiting...

To fix this, make sure the Ledger node is available from where you’re running the Extractor.

Can’t connect to the database
=============================

If the database isn’t available before the transaction stream is started, the Extractor will terminate, and you’ll see the error from the JDBC driver in the logs::

  17:19:12.071 ERROR c.d.e.Main$@[kka.actor.default-dispatcher-5] - FAILURE:
  org.postgresql.util.PSQLException: FATAL: database "192.153.1.23:daml_export" does not exist.
  Exiting…

To fix this, make sure make sure the database exists and is available from where you’re running the Extractor, the username and password your using are correct, and you have the credentials to connect to the database from the network address where the you’re running the Extractor.

If the database connection is broken while the transaction stream was already running, you’ll see a similar message in the logs, but in this case it will be repeated: as explained in the `Fault tolerance <#fault-tolerance>`__ section, the transaction stream will be restarted with an exponential backoff, giving the database, network or any other trouble resource to get back into shape. Once everything’s back in order, the stream will continue without any need for manual intervention.
