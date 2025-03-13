##########################
Ledger API migration guide
##########################

***********
Version 3.3
***********

Domain to Synchronizer rename
-----------------------------

Pure renaming without changing the semantics.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed endpoint
      - Migration instruction
   -

      - GetConnectedDomains
      - Endpoint renamed to ``GetConnectedSynchronizers``.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed message
      - Migration instruction
   -

      - DomainTime, GetConnectedDomainsRequest, GetConnectedDomainsResponse, ConnectedDomain, Transaction, Metadata, PrepareSubmissionRequest
      - Messages renamed: ``Domain*`` to ``Synchronizer*``.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - DisclosedContract, Commands, Created, Archived, ActiveContract, TransactionTree
      - Field ``domain_id`` is consistently renamed to ``synchronizer_id``.
   -

      - Completion
      - Field ``domain_time`` is renamed to ``synchronizer_time``.
   -

      - OffsetCheckpoint
      - Field ``domain_times`` renamed to ``synchronizer_times``.
   -

      - ConnectedSynchronizer
      - Fields renamed: ``domain`` to ``synchronizer``.

Event ID to offset and node_id
------------------------------

Field ``event_id`` is dropped in favour of two newly introduced fields: ``offset`` and ``node_id``. These two properties identify the events precisely.

The structural representation of daml transaction trees changed: instead of exposing the root event IDs and the children for exercised events, only the last descendant node ID is exposed.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed endpoint
      - Migration instruction
   -

      - GetTransactionTreeByEventId, GetTransactionByEventId
      - The ``*ByEventId`` pointwise-lookup endpoints changed, they are renamed to ``*ByOffset`` respectively. The lookups should be based on the newly introduced ``offset`` field, instead of the ``event_id`` from the respective ``Event``.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - CreatedEvent, ArchivedEvent, ExercisedEvent
      - Field ``event_id`` is removed in favor of new field ``offset`` (referencing the offset of origin) and new field ``node_id``. These two fields identify the event.
   -

      - ExercisedEvent
      - Field ``child_event_ids`` is removed. Instead the new field ``last_descendant_node_id`` can be used, which captures structural information of the transaction trees.
   -

      - TransactionTree
      - Field ``events_by_id`` changed its type: the key of the map was previously a ``string`` referring to ``event_id``\ ’s, now it is an ``int32`` referring to the ``node_id``\ ’s of the ``Event’``\ s.

        Field ``root_event_ids`` is removed. The root nodes can be now computed with the help of the ordered list of events and the ``last_descendant_id``. If Java-bindings are used there is a helper function ``getRootNodeIds()``.

Universal Streams
-----------------

Broad set of changes targeting consolidation of the flat and tree style distinction. Mostly backwards compatible changes, but clients need to migrate to the new API, because the old API will be removed in future version 3.4.

The incompatible changes that are already materialized in the version 3.3
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Changed endpoint
      - Migration instruction
   -

      - SubmitAndWaitForTransaction
      - Instead of ``SubmitAndWaitRequest`` a new message ``SubmitAndWaitForTransactionRequest`` needs to be used. Here it is also mandatory to specify the ``transaction_format`` field.

        To retain the original behavior, ``transaction_format`` field should be defined with:

        - ``transaction_shape`` set to ``ACS_DELTA``
        - ``event_format`` defined with:

            - ``filters_by_party`` containing wildcard-template filter for all original ``Commands.act_as`` parties
            - ``verbose`` flag set
   -

      - GetEventsByContractId
      - In case of no events found, previously a ``GetEventsByContractIdResponse`` is returned with empty ``created`` and empty ``archived`` fields, now a ``CONTRACT_NOT_FOUND`` error is raised.

        Additionally: if ``EventFormat`` would filter out all results, a ``CONTRACT_NOT_FOUND`` error would be raised as well.
   -

      - GetTransactionByOffset, GetTransactionById
      - The behavior of these endpoints changed: instead of returning an empty transaction, a ``TRANSACTION_NOT_FOUND`` error will be raised, if the transaction cannot be found, or filtering based on the query parameters would lead to an empty transaction.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - Event
      - The ``oneof event`` is extended with ``exercised`` to support universal streams. The values of the events in case of ``transaction_shape``:

        - ``LEDGER_EFFECTS`` can be ``created`` or ``exercised``
        - ``ACS_DELTA`` can be ``created`` or ``archived``
   -

      - GetUpdatesResponse
      - The update ``oneof`` is extended to accommodate new ``TopologyTransaction`` updates. These types of messages will be only populated if the experimental flag is turned on, and the respective field of the ``update_filter`` is set.

The changes that need to be adopted by the users of 3.3 so that they are ready for version 3.4 where the legacy API will be removed.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The protobuf definitions contain documentation markers for the places where an endpoint, a message or a field will be removed in 3.4. Look for the following formulation:

::

   // TODO(i23504) Provided for backwards compatibility, it will be removed in the final version.

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   -

      - Deprecated endpoint
      - Migrating to
      - Migration instruction
   -

      - SubmitAndWaitForTransactionTree
      - SubmitAndWaitForTransaction
      - ``transaction_format`` with ``transaction_shape`` ``LEDGER_EFFECTS``
   -

      - GetUpdateTrees
      - GetUpdates
      - ``include_transactions`` with ``transaction_shape LEDGER_EFFECTS``
   -

      - GetTransactionTreeByOffset
      - GetTransactionByOffset
      - ``transaction_format`` with ``transaction_shape LEDGER_EFFECTS``
   -

      - GetTransactionTreeById
      - GetTransactionById
      - ``transaction_format`` with ``transaction_shape LEDGER_EFFECTS``

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Deprecated message
      - Migration instruction
   -

      - SubmitAndWaitForTransactionTreeResponse
      - ``SubmitAndWaitForTransaction`` should be used with setting ``transaction_format`` with ``transaction_shape`` ``LEDGER_EFFECTS`` in the ``SubmitAndWaitForTransactionRequest``
   -

      - TreeEvent, TransactionTree, GetUpdateTreesResponse, GetTransactionTreeResponse
      - These were used in tree\* query/streaming scenarios, which is deprecated. The respective non tree\* endpoint should be used with setting transaction_shape to ``LEDGER_EFFECTS`` in the request, and therefore resulting ``Event``-s will be either ``CreatedEvent`` or ``ExercisedEvent``.
   -

      - TransactionFilter
      - ``EventFormat`` should be used, which contains the original fields from ``TransactionFilter`` plus the ``verbose`` flag.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with deprecated field(s)
      - Migration instruction
   -

      - GetEventsByContractIdRequest
      - The new ``event_format`` field should be used instead of the deprecated ``requesting_parties``.

        To retain the original behavior, event format field should be defined with:

        - ``filters_by_party`` should be wildcard filter for all
        - ``requesting_parties`` ``verbose`` should be unset
   -

      - GetActiveContractsRequest
      - The new ``event_format`` field should be used instead of the deprecated filter and verbose fields.

        To retain the original behavior, the ``event_format`` field should be defined with:

        - ``filters_by_party`` and ``filters_for_any_party`` should be the same as in the original ``filter`` field
        - ``verbose`` should be the same as the original ``verbose`` field
   -

      - GetUpdatesRequest
      - The new ``update_format`` field should be used instead of the deprecated ``filter`` and ``verbose`` fields.

        To retain the original behavior:

        - ``include_transactions`` should be defined with

            - ``transaction_shape`` set to ``ACS_DELTA``
            - ``event_format`` should be defined by the original ``filter`` and ``verbose`` fields

        - ``include_reassignments`` should be defined by the original ``filter`` and ``verbose`` fields
        - ``include_topology_events`` should be unset
   -

      - GetTransactionByOffsetRequest, GetTransactionByIdRequest
      - The new ``transaction_format`` field should be used instead of the ``requesting_parties`` field.

        To retain the original behavior, ``transaction_format`` field should be defined with:

        - ``transaction_shape`` set to ``ACS_DELTA``
        - ``event_format`` should be defined with:

            - ``filters_by_party`` should be wildcard filter for all original ``requesting_parties``
            - ``verbose`` should be set

Interactive submission
----------------------

The interactive submission related proto definitions moved to the interactive folder.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - ExecuteSubmissionRequest
      - The ``min_ledger_time`` field has been removed as it was unused. ``min_ledger_time`` can be set in the PrepareSubmissionRequest message instead.

Various
-------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   -

      - Message with changed field(s)
      - Migration instruction
   -

      - Completion
      - Field ``deduplication_offset`` can have value zero, with meaning: participant begin.
