.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _custom-views:

Custom Views
############

Custom views provides convenient features to continuously ingest data from a Ledger into a database,
into tables of your choice, optimized for your querying requirements.

Custom views is a Java library for **projecting** Ledger events into a SQL database.
It is currently available in Labs early access, and only supports PostgreSQL right now.

Use the following Maven dependency to use the Custom Views library:

.. code-block:: xml

    <dependency>
        <groupId>com.daml</groupId>
        <artifactId>custom-views_2.13</artifactId>
        <version>2.5.0</version>
    </dependency>

Please see the `Custom Views github repository <https://github.com/digital-asset/custom-views/>`_ for an example Maven project of a simple application
that projects data into a SQL table and provides access to the data through a REST interface.

Overview
********

A **Projection** is a resumable process that continuously reads ledger events and transforms these into rows in SQL tables.
A projection ensures that rows are committed according to ledger transactions,
ensuring that isolation and atomicity of changes perceived by database users is consistent with committed transactions on the ledger.

At a high level, the following types are needed to run a projection process:

- A ``BatchSource`` connects to the Ledger and reads events from it.
- A ``Projection`` defines which events to process from the ``BatchSource``, from which ``Offset`` to start processing, optionally up to an end ``Offset``.
- A ``Project`` function converts the events into database actions. A ``JdbcAction`` type can be used to define which SQL statements should be executed.
- A ``Projector`` takes a ``BatchSource``, a ``Projection``, and a ``Project`` function, to read and transform events into actions, and executes these. Database transactions are committed as they occurred on the ledger.

A common workflow for setting up a projection process follows:

- Create a table in your SQL database.
- Create a ``Projector``.
- Choose the type of event you want to project and create a ``BatchSource`` for it.
- Create a ``Projection``. If the projection already exists, it will continue where it left off.
- Create a ``Project`` function that transforms an event into (0 to N) database actions.
- invoke ``project`` on the ``Projector``, passing in the ``BatchSource``, the ``Projection``, and the ``Project`` function. This starts the projection process, and returns a ``Control`` to control the process.
- cancel the projection by invoking ``control.cancel`` on shutdown of your application.

The next sections will explain the most important objects in the Custom Views library in more detail.

Projector
*********

A ``Projector`` executes the projection process. The code snippet below shows how to create a ``JdbcProjector``.

.. code-block:: java

    ConnectionSupplier connectionSupplier =
      () -> {
        return java.sql.DriverManager.getConnection(url, user, password);
      };
    var system = ActorSystem.create("my-projection-app");
    var projector = JdbcProjector.create(connectionSupplier, system);

A ``Projector`` provides ``project`` methods to start a projection process.
The `ConnectionSupplier` is used to create database connections when required.
The ``project`` methods return a ``Control`` which can be used to:

- cancel the projection
- find out if the projection has completed or failed
- wait for the projection process to close all its resources.

A projection only completes if an end ``Offset`` is set, otherwise it will continuously run and project events as they occur on the Ledger.
The ``project`` methods take a ``BatchSource``, a ``Projection`` and a ``Project`` function, which are explained in the next sections.

BatchSource
===========
A projection connects to the Ledger and reads events using a ``BatchSource``, which internally uses the Ledger gRPC client API.
The Ledger API provides the following types of events:

- ``Event`` (``CreatedEvent`` or ``ArchivedEvent``)
- ``ExercisedEvent``
- ``TreeEvent``

The projection library uses the ``com.daml.ledger.javaapi.data`` ``Event``, ``ExercisedEvent`` and ``TreeEvent`` classes from the :doc:`Java Bindings</app-dev/bindings-java/index>`
to represent these events.

The following `BatchSources` are available:

- ``BatchSource.events`` creates a ``BatchSource`` that reads ``Event``\s from the ledger.
- ``BatchSource.exercisedEvents`` creates a ``BatchSource`` that reads ``ExercisedEvent``\s from the ledger.
- ``BatchSource.treeEvents`` creates a ``BatchSource`` that reads ``TreeEvent``\s from the ledger.

The example below shows how to create a ``BatchSource`` that will read ``CreatedEvent``\s and ``ArchivedEvent``\s from the Ledger at ``localhost``, port ``6865``:

.. code-block:: java

    var grpcClientSettings = GrpcClientSettings.connectToServiceAt("localhost", 6865, system);
    var source = BatchSource.events(grpcClientSettings);

Additionally ``BatchSource.create`` can be used to create a ``BatchSource`` of a code-generated ``Contract`` types from ``CreateEvent``\s,
or to create a ``BatchSource`` from simple values, which is convenient for unit testing.

Batch
-----

A ``BatchSource`` reads events into `Batches`. A ``Batch`` consists of 1 to many events, and optionally contains a marker that indicates that a transaction has been committed on the Ledger.
`Batches` make it possible to process larger than memory transactions, while tracking transactions as they occur on the ledger, and making it possible for downstream
database transactions to only commit when these transaction markers have been detected.

Envelope
--------

The events in `Batches` are wrapped in `Envelopes`. An ``Envelope`` provides additional fields providing more context about what occurred on the Ledger.
It has the following fields:

- event: The wrapped value. ``getEvent`` and ``unwrap()`` both provide this value.
- offset: The offset of the event.
- table: The (main) ``ProjectionTable`` that is projected to.
- workflowId (optional)
- ledgerEffectiveTime (optional)
- transactionId (optional)

Projection
==========

The Projection keeps track of the projection process and decides which events will be projected from the BatchSource.

A Projection:

- has a `ProjectionId` that must uniquely identify the projection process.
- has an ``Offset`` which is used as a starting point to read from the ledger.
- has a ``ProjectionFilter``. The ``BatchSource`` uses this filter to select events from the Ledger. (If you are familiar with the gRPC service, the ``ProjectionFilter`` translates to a ``TransactionFilter``)
- specifies an SQL table to project to with a ``ProjectionTable``.
- optionally has a ``Predicate`` to filter events that were read from the Ledger.
- optionally has an end ``Offset``, if set the projection will end when a transaction for the ``Offset`` has been read from the Ledger.
- is stored in the ``projection`` SQL table.

A newly created projection by default has no offset, which means a projection will start from the beginning of the Ledger.
A projection is updated when it successfully commits transactions into the SQL database according to transactions that were committed on the Ledger.
A projection will be resumed from its stored offset automatically, if it can be found by its `ProjectionId`.

The code below shows an example of how to create a `Projection`:

.. code-block:: java

    var projectionTable = new ProjectionTable("ious");
    var eventsProjection =
      Projection.<Event>create(
        new ProjectionId("iou-projection-for-party"),
        ProjectionFilter.parties(Set.of(partyId)),
        projectionTable
      );

The ``eventsProjection`` ``Projection`` selects ``Event``\s that occurred visible to the party ``partyId`` to the ``ious`` SQL table.

The Project function
====================

The `Project<E,A>` function projects an event `Envelope<E>` into a `List<A>`.
For the ``project`` methods on ``JdbcProjector``, `A` is a ``JdbcAction``.

The code below shows an example of a ``Project`` function that handles `CreatedEvents` and `ArchivedEvents`.

.. code-block:: java

    Project<Event, JdbcAction> f =
      envelope -> {
        var event = envelope.getEvent();
        if (event instanceof CreatedEvent) {
          Iou.Contract iou = Iou.Contract.fromCreatedEvent((CreatedEvent) event);
          var action =
            ExecuteUpdate.create(
              "insert into "
              + envelope.getProjectionTable().getName()
              + "(contract_id, event_id, amount, currency) "
              + "values (?, ?, ?, ?)"
            )
            .bind(1, event.getContractId())
            .bind(2, event.getEventId())
            .bind(3, iou.data.amount)
            .bind(4, iou.data.currency);
          return List.of(action);
        } else {
          var action =
            ExecuteUpdate.create(
              "delete from " +
              envelope.getProjectionTable().getName() +
              " where contract_id = ?"
            )
            .bind(1, event.getContractId());
          return List.of(action);
        }
      };

The Project function `f` creates an insert action for every ``CreatedEvent`` and a delete action for every ``ArchivedEvent``.
The Jdbc actions are further explained in the next section.

The JdbcAction
--------------

A database action captures a SQL statement that is executed by a ``Projector``.
The `JdbcAction` is an interface with one method, shown in the example below:

.. code-block:: java

    public int execute(java.sql.Connection con);

All ``JdbcAction``\s extend ``JdbcAction``. ``execute`` should return the number of rows affected by the action.
The ``ExecuteUpdate`` action can be used to create an insert, delete or update statement.
The example below shows how an insert statement can be created, and how arguments can be bound to the statement:

.. code-block:: java

    ExecuteUpdate.create(
        "insert into "
        + envelope.getProjectionTable().getName()
        + "(contract_id, event_id, amount, currency) "
        + "values (?, ?, ?, ?)")
        .bind(1, event.getContractId())
        .bind(2, event.getEventId())
        .bind(3, iou.data.amount)
        .bind(4, iou.data.currency);

It is also possible to use named parameters, which is shown in the example below:

.. code-block:: java

    ExecuteUpdate.create(
        "insert into "
        + envelope.getProjectionTable().getName()
        + "(contract_id, event_id, amount, currency) "
        + "values (:cid, :eid, :amount, :currency)")
        .bind("cid", event.getContractId())
        .bind("eid", event.getEventId())
        .bind("amount", iou.data.amount)
        .bind("currency", iou.data.currency);

Projecting rows in batches
--------------------------

The `ExecuteUpdate` action internally creates a new ``java.sql.PreparedStatement`` when it is executed.
Use `UpdateMany` If you want to reuse the ``java.sql.PreparedStatement`` and add statements in batches, which can make a considerable difference in performance.
The example below shows how you can use ``projectRows`` to project using ``UpdateMany``.
In this case we are using a code generated ``Iou.Contract`` class to function as a `Row`, which we use to bind to a SQL statement
which is executed in batches.

.. code-block:: java

    var projectionTable = new ProjectionTable("ious");
    var contracts = Projection.<Iou.Contract>create(
      new ProjectionId("iou-contracts-for-party"),
      ProjectionFilter.parties(Set.of(partyId)),
      projectionTable
    );
    var batchSource = BatchSource.create(grpcClientSettings,
        e -> {
          return Iou.Contract.fromCreatedEvent(e);
        });
    Project<Iou.Contract, Iou.Contract> mkRow =
        envelope -> {
          return List.of(envelope.getEvent());
        };
    Binder<Iou.Contract> binder = Sql.<Iou.Contract>binder(
        "insert into "
        + projectionTable.getName()
        + "(contract_id, event_id, amount, currency) "
        + "values (:contract_id, :event_id, :amount, :currency)")
        .bind("contract_id", iou -> iou.id.contractId)
        .bind("event_id", iou -> null)
        .bind("amount", iou -> iou.data.amount)
        .bind("currency", iou -> iou.data.currency);
    BatchRows<Iou.Contract, JdbcAction> batchRows =
        UpdateMany.create(binder);
    var control =
        projector.projectRows(
            batchSource,
            contracts,
            batchRows,
            mkRow
        );

The ``Project`` function just returns the ``Iou.Contract`` since we can use this directly for our insert statement.
Next we use a ``Binder`` to bind the ``Iou.Contract`` to the insert statement.
The ``UpdateMany.create`` creates a ``BatchRow``\s function that transforms a ``List`` of rows, in this case ``Iou.Contract``\s, into a single ``JdbcAction``.
``projectRows`` starts the projection process, converting created ``Iou.Contract``\s into rows in the ``ious`` table.

Configuration
*************

The Custom Views library uses the `Lightbend config library <https://github.com/lightbend/config>`_ for configuration.
The library is packaged with a ``reference.conf`` file which specifies default settings. The next sections describe the default confguration settings.
You can override the configuration by using an ``application.conf`` file, see `using the Lightbend config library <https://github.com/lightbend/config#using-the-library>`_ for more details.

Batcher configuration
=====================

A ``Batch`` consists of 1 to many events, and optionally contains a marker that indicates that a transaction has been committed on the Ledger.

Both the ``batch-size`` and the ``batch-interval`` is configured in the reference.conf

.. code-block:: none

    projection {
      batch-size = 10000
      batch-interval = 1 second
    }

Dispatcher configuration for blocking operation
===============================================

A default dedicated dispatcher for blocking operations (e.g. db operation) is configured in reference.conf:

.. code-block:: none

    projection {
      blocking-io-dispatcher {
        type = Dispatcher
        executor = "thread-pool-executor"
        thread-pool-executor {
          fixed-pool-size = 16
        }
        throughput = 1
      }
    }
