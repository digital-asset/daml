Ledger API Reference
####################


.. _com/digitalasset/ledger/api/v1/active_contracts_service.proto:

com/digitalasset/ledger/api/v1/active_contracts_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.GetActiveContractsRequest:

GetActiveContractsRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsRequest.filter: 

       filter
     - :ref:`TransactionFilter <com.digitalasset.ledger.api.v1.TransactionFilter>`
     - 
     - Templates to include in the served snapshot, per party. Required
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsRequest.verbose: 

       verbose
     - :ref:`bool <bool>`
     - 
     - If enabled, values served over the API will contain more information than strictly necessary to interpret the data. In particular, setting the verbose flag to true triggers the ledger to include labels for record fields. Optional
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetActiveContractsResponse:

GetActiveContractsResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsResponse.offset: 

       offset
     - :ref:`string <string>`
     - 
     - Included in the last message. The client should start consuming the transactions endpoint with this offset. The format of this field is described in ``ledger_offset.proto``. Required
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsResponse.workflow_id: 

       workflow_id
     - :ref:`string <string>`
     - 
     - The workflow that created the contracts. Optional
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsResponse.active_contracts: 

       active_contracts
     - :ref:`CreatedEvent <com.digitalasset.ledger.api.v1.CreatedEvent>`
     - repeated
     - The list of contracts that were introduced by the workflow with ``workflow_id`` at the offset. Optional
   * - .. _com.digitalasset.ledger.api.v1.GetActiveContractsResponse.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Zipkin trace context. This field is a future extension point and is currently not supported. Optional
   






.. _com.digitalasset.ledger.api.v1.ActiveContractsService:

ActiveContractsService
===================================================================================================

Allows clients to initialize themselves according to a fairly recent state of the ledger without reading through all transactions that were committed since the ledger's creation.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - GetActiveContracts
     - :ref:`GetActiveContractsRequest <com.digitalasset.ledger.api.v1.GetActiveContractsRequest>`
     - :ref:`GetActiveContractsResponse <com.digitalasset.ledger.api.v1.GetActiveContractsResponse>`
     - Returns a stream of the latest snapshot of active contracts. Getting an empty stream means that the active contracts set is empty and the client should listen to transactions using ``LEDGER_BEGIN``. Clients SHOULD NOT assume that the set of active contracts they receive reflects the state at the ledger end.
   



.. _com/digitalasset/ledger/api/v1/command_completion_service.proto:

com/digitalasset/ledger/api/v1/command_completion_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.Checkpoint:

Checkpoint
===================================================================================================

Checkpoints may be used to:

* detect time out of commands.
* provide an offset which can be used to restart consumption.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Checkpoint.record_time: 

       record_time
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - All commands with a maximum record time below this value MUST be considered lost if their completion has not arrived before this checkpoint. Required
   * - .. _com.digitalasset.ledger.api.v1.Checkpoint.offset: 

       offset
     - :ref:`LedgerOffset <com.digitalasset.ledger.api.v1.LedgerOffset>`
     - 
     - May be used in a subsequent CompletionStreamRequest to resume the consumption of this stream at a later time. Required
   



.. _com.digitalasset.ledger.api.v1.CompletionEndRequest:

CompletionEndRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CompletionEndRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.CompletionEndRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.CompletionEndResponse:

CompletionEndResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CompletionEndResponse.offset: 

       offset
     - :ref:`LedgerOffset <com.digitalasset.ledger.api.v1.LedgerOffset>`
     - 
     - This offset can be used in a CompletionStreamRequest message. Required
   



.. _com.digitalasset.ledger.api.v1.CompletionStreamRequest:

CompletionStreamRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CompletionStreamRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger id reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.CompletionStreamRequest.application_id: 

       application_id
     - :ref:`string <string>`
     - 
     - Only completions of commands submitted with the same application_id will be visible in the stream. Required
   * - .. _com.digitalasset.ledger.api.v1.CompletionStreamRequest.parties: 

       parties
     - :ref:`string <string>`
     - repeated
     - Non-empty list of parties whose data should be included. Required
   * - .. _com.digitalasset.ledger.api.v1.CompletionStreamRequest.offset: 

       offset
     - :ref:`LedgerOffset <com.digitalasset.ledger.api.v1.LedgerOffset>`
     - 
     - This field indicates the minimum offset for completions. This can be used to resume an earlier completion stream. Optional, if not set the ledger uses the current ledger end offset instead.
   



.. _com.digitalasset.ledger.api.v1.CompletionStreamResponse:

CompletionStreamResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CompletionStreamResponse.checkpoint: 

       checkpoint
     - :ref:`Checkpoint <com.digitalasset.ledger.api.v1.Checkpoint>`
     - 
     - This checkpoint may be used to restart consumption. The checkpoint is after any completions in this response. Optional
   * - .. _com.digitalasset.ledger.api.v1.CompletionStreamResponse.completions: 

       completions
     - :ref:`Completion <com.digitalasset.ledger.api.v1.Completion>`
     - repeated
     - If set, one or more completions.
   






.. _com.digitalasset.ledger.api.v1.CommandCompletionService:

CommandCompletionService
===================================================================================================

Allows clients to observe the status of their submissions.
Commands may be submitted via the Command Submission Service.
The on-ledger effects of their submissions are disclosed by the Transaction Service.
Commands may fail in 4 distinct manners:

1. ``INVALID_PARAMETER`` gRPC error on malformed payloads and missing required fields.
2. Failure communicated in the gRPC error.
3. Failure communicated in a Completion.
4. A Checkpoint with ``record_time`` > command ``mrt`` arrives through the Completion Stream, and the command's Completion was not visible before. In this case the command is lost.

Clients that do not receive a successful completion about their submission MUST NOT assume that it was successful.
Clients SHOULD subscribe to the CompletionStream before starting to submit commands to prevent race conditions.

Interprocess tracing of command submissions may be achieved via Zipkin by filling out the ``trace_context`` field.
The server will return a child context of the submitted one, (or a new one if the context was missing) on both the Completion and Transaction streams.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - CompletionStream
     - :ref:`CompletionStreamRequest <com.digitalasset.ledger.api.v1.CompletionStreamRequest>`
     - :ref:`CompletionStreamResponse <com.digitalasset.ledger.api.v1.CompletionStreamResponse>`
     - Subscribe to command completion events.
   * - CompletionEnd
     - :ref:`CompletionEndRequest <com.digitalasset.ledger.api.v1.CompletionEndRequest>`
     - :ref:`CompletionEndResponse <com.digitalasset.ledger.api.v1.CompletionEndResponse>`
     - Returns the offset after the latest completion.
   



.. _com/digitalasset/ledger/api/v1/command_service.proto:

com/digitalasset/ledger/api/v1/command_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.SubmitAndWaitRequest:

SubmitAndWaitRequest
===================================================================================================

These commands are atomic, and will become transactions.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.SubmitAndWaitRequest.commands: 

       commands
     - :ref:`Commands <com.digitalasset.ledger.api.v1.Commands>`
     - 
     - The commands to be submitted. Required
   * - .. _com.digitalasset.ledger.api.v1.SubmitAndWaitRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   






.. _com.digitalasset.ledger.api.v1.CommandService:

CommandService
===================================================================================================

Command Service is able to correlate submitted commands with completion data, identify timeouts, and return contextual
information with each tracking result. This supports the implementation of stateless clients.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - SubmitAndWait
     - :ref:`SubmitAndWaitRequest <com.digitalasset.ledger.api.v1.SubmitAndWaitRequest>`
     -  `.google.protobuf.Empty <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Empty>`__
     - Submits a single composite command and waits for its result. Returns ``RESOURCE_EXHAUSTED`` if the number of in-flight commands reached the maximum (if a limit is configured). Propagates the gRPC error of failed submissions including DAML interpretation errors.
   



.. _com/digitalasset/ledger/api/v1/command_submission_service.proto:

com/digitalasset/ledger/api/v1/command_submission_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.SubmitRequest:

SubmitRequest
===================================================================================================

The submitted commands will be processed atomically in a single transaction. Moreover, each ``Command`` in ``commands`` will be executed in the order specified by the request.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.SubmitRequest.commands: 

       commands
     - :ref:`Commands <com.digitalasset.ledger.api.v1.Commands>`
     - 
     - The commands to be submitted in a single transaction. Required
   * - .. _com.digitalasset.ledger.api.v1.SubmitRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   






.. _com.digitalasset.ledger.api.v1.CommandSubmissionService:

CommandSubmissionService
===================================================================================================

Allows clients to attempt advancing the ledger's state by submitting commands.
The final states of their submissions are disclosed by the Command Completion Service.
The on-ledger effects of their submissions are disclosed by the Transaction Service.
Commands may fail in 4 distinct manners:

1) ``INVALID_PARAMETER`` gRPC error on malformed payloads and missing required fields.
2) Failure communicated in the gRPC error.
3) Failure communicated in a Completion.
4) A Checkpoint with ``record_time`` > command ``mrt`` arrives through the Completion Stream, and the command's Completion was not visible before. In this case the command is lost.

Clients that do not receive a successful completion about their submission MUST NOT assume that it was successful.
Clients SHOULD subscribe to the CompletionStream before starting to submit commands to prevent race conditions.

Interprocess tracing of command submissions may be achieved via Zipkin by filling out the ``trace_context`` field.
The server will return a child context of the submitted one, (or a new one if the context was missing) on both the Completion and Transaction streams.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - Submit
     - :ref:`SubmitRequest <com.digitalasset.ledger.api.v1.SubmitRequest>`
     -  `.google.protobuf.Empty <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Empty>`__
     - Submit a single composite command.
   



.. _com/digitalasset/ledger/api/v1/commands.proto:

com/digitalasset/ledger/api/v1/commands.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.Command:

Command
===================================================================================================

A command can either create a new contract or exercise a choice on an existing contract.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Command.create: 

       create
     - :ref:`CreateCommand <com.digitalasset.ledger.api.v1.CreateCommand>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Command.exercise: 

       exercise
     - :ref:`ExerciseCommand <com.digitalasset.ledger.api.v1.ExerciseCommand>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Command.createAndExercise: 

       createAndExercise
     - :ref:`CreateAndExerciseCommand <com.digitalasset.ledger.api.v1.CreateAndExerciseCommand>`
     - 
     - 
   



.. _com.digitalasset.ledger.api.v1.Commands:

Commands
===================================================================================================

A composite command that groups multiple commands together.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Commands.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.Commands.workflow_id: 

       workflow_id
     - :ref:`string <string>`
     - 
     - Identifier of the on-ledger workflow that this command is a part of. Optional
   * - .. _com.digitalasset.ledger.api.v1.Commands.application_id: 

       application_id
     - :ref:`string <string>`
     - 
     - Uniquely identifies the application (or its part) that issued the command. This is used in tracing across different components and to let applications subscribe to their own submissions only. Required
   * - .. _com.digitalasset.ledger.api.v1.Commands.command_id: 

       command_id
     - :ref:`string <string>`
     - 
     - Unique command ID. This number should be unique for each new command within an application domain. It can be used for matching the requests with their respective completions. Required
   * - .. _com.digitalasset.ledger.api.v1.Commands.party: 

       party
     - :ref:`string <string>`
     - 
     - Party on whose behalf the command should be executed. It is up to the server to verify that the authorisation can be granted and that the connection has been authenticated for that party. Required
   * - .. _com.digitalasset.ledger.api.v1.Commands.ledger_effective_time: 

       ledger_effective_time
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - MUST be an approximation of the wall clock time on the ledger server. Required
   * - .. _com.digitalasset.ledger.api.v1.Commands.maximum_record_time: 

       maximum_record_time
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - The deadline for observing this command in the completion stream before it can be considered to have timed out. Required
   * - .. _com.digitalasset.ledger.api.v1.Commands.commands: 

       commands
     - :ref:`Command <com.digitalasset.ledger.api.v1.Command>`
     - repeated
     - Individual elements of this atomic command. Must be non-empty. Required
   



.. _com.digitalasset.ledger.api.v1.CreateAndExerciseCommand:

CreateAndExerciseCommand
===================================================================================================

Create a contract and exercise a choice on it in the same transaction.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CreateAndExerciseCommand.template_id: 

       template_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - The template of the contract the client wants to create Required
   * - .. _com.digitalasset.ledger.api.v1.CreateAndExerciseCommand.create_arguments: 

       create_arguments
     - :ref:`Record <com.digitalasset.ledger.api.v1.Record>`
     - 
     - The arguments required for creating a contract from this template. Required
   * - .. _com.digitalasset.ledger.api.v1.CreateAndExerciseCommand.choice: 

       choice
     - :ref:`string <string>`
     - 
     - The name of the choice the client wants to exercise. Required
   * - .. _com.digitalasset.ledger.api.v1.CreateAndExerciseCommand.choice_argument: 

       choice_argument
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - The argument for this choice. Required
   



.. _com.digitalasset.ledger.api.v1.CreateCommand:

CreateCommand
===================================================================================================

Create a new contract instance based on a template.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CreateCommand.template_id: 

       template_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - The template of contract the client wants to create. Required
   * - .. _com.digitalasset.ledger.api.v1.CreateCommand.create_arguments: 

       create_arguments
     - :ref:`Record <com.digitalasset.ledger.api.v1.Record>`
     - 
     - The arguments required for creating a contract from this template. Required
   



.. _com.digitalasset.ledger.api.v1.ExerciseCommand:

ExerciseCommand
===================================================================================================

Exercise a choice on an existing contract.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.ExerciseCommand.template_id: 

       template_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - The template of contract the client wants to exercise. Required
   * - .. _com.digitalasset.ledger.api.v1.ExerciseCommand.contract_id: 

       contract_id
     - :ref:`string <string>`
     - 
     - The ID of the contract the client wants to exercise upon. Required
   * - .. _com.digitalasset.ledger.api.v1.ExerciseCommand.choice: 

       choice
     - :ref:`string <string>`
     - 
     - The name of the choice the client wants to exercise. Required
   * - .. _com.digitalasset.ledger.api.v1.ExerciseCommand.choice_argument: 

       choice_argument
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - The argument for this choice. Required
   








.. _com/digitalasset/ledger/api/v1/completion.proto:

com/digitalasset/ledger/api/v1/completion.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.Completion:

Completion
===================================================================================================

A completion represents the status of a submitted command on the ledger: it can be successful or failed.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Completion.command_id: 

       command_id
     - :ref:`string <string>`
     - 
     - The ID of the succeeded or failed command. Required
   * - .. _com.digitalasset.ledger.api.v1.Completion.status: 

       status
     -  `google.rpc.Status <https://cloud.google.com/tasks/docs/reference/rpc/google.rpc#google.rpc.Status>`__
     - 
     - Identifies the exact type of the error. For example, malformed or double spend transactions will result in a ``INVALID_ARGUMENT`` status. Transactions with invalid time time windows (which may be valid at a later date) will result in an ``ABORTED`` error. Optional
   * - .. _com.digitalasset.ledger.api.v1.Completion.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - The trace context submitted with the command. This field is a future extension point and is currently not supported. Optional
   








.. _com/digitalasset/ledger/api/v1/event.proto:

com/digitalasset/ledger/api/v1/event.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.ArchivedEvent:

ArchivedEvent
===================================================================================================

Records that a contract has been archived, and choices may no longer be exercised on it.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.ArchivedEvent.event_id: 

       event_id
     - :ref:`string <string>`
     - 
     - The ID of this particular event. Required
   * - .. _com.digitalasset.ledger.api.v1.ArchivedEvent.contract_id: 

       contract_id
     - :ref:`string <string>`
     - 
     - The ID of the archived contract. Required
   * - .. _com.digitalasset.ledger.api.v1.ArchivedEvent.template_id: 

       template_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - The template of the archived contract. Required
   * - .. _com.digitalasset.ledger.api.v1.ArchivedEvent.witness_parties: 

       witness_parties
     - :ref:`string <string>`
     - repeated
     - The parties that are notified of this event. Required
   



.. _com.digitalasset.ledger.api.v1.CreatedEvent:

CreatedEvent
===================================================================================================

Records that a contract has been created, and choices may now be exercised on it.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.CreatedEvent.event_id: 

       event_id
     - :ref:`string <string>`
     - 
     - The ID of this particular event. Required
   * - .. _com.digitalasset.ledger.api.v1.CreatedEvent.contract_id: 

       contract_id
     - :ref:`string <string>`
     - 
     - The ID of the created contract. Required
   * - .. _com.digitalasset.ledger.api.v1.CreatedEvent.template_id: 

       template_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - The template of the created contract. Required
   * - .. _com.digitalasset.ledger.api.v1.CreatedEvent.create_arguments: 

       create_arguments
     - :ref:`Record <com.digitalasset.ledger.api.v1.Record>`
     - 
     - The arguments that have been used to create the contract. Required
   * - .. _com.digitalasset.ledger.api.v1.CreatedEvent.witness_parties: 

       witness_parties
     - :ref:`string <string>`
     - repeated
     - The parties that are notified of this event. Required
   



.. _com.digitalasset.ledger.api.v1.Event:

Event
===================================================================================================

An event on the ledger can either be the creation or the archiving of a contract, or the exercise of a choice on a contract.
The ``GetTransactionTrees`` response will only contain create and exercise events.
Archive events correspond to consuming exercise events.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Event.created: 

       created
     - :ref:`CreatedEvent <com.digitalasset.ledger.api.v1.CreatedEvent>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Event.exercised: 

       exercised
     - :ref:`ExercisedEvent <com.digitalasset.ledger.api.v1.ExercisedEvent>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Event.archived: 

       archived
     - :ref:`ArchivedEvent <com.digitalasset.ledger.api.v1.ArchivedEvent>`
     - 
     - 
   



.. _com.digitalasset.ledger.api.v1.ExercisedEvent:

ExercisedEvent
===================================================================================================

Records that a choice has been exercised on a target contract.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.event_id: 

       event_id
     - :ref:`string <string>`
     - 
     - The ID of this particular event. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.contract_id: 

       contract_id
     - :ref:`string <string>`
     - 
     - The ID of the target contract. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.template_id: 

       template_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - The template of the target contract. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.contract_creating_event_id: 

       contract_creating_event_id
     - :ref:`string <string>`
     - 
     - The ID of the event in which the target contract has been created. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.choice: 

       choice
     - :ref:`string <string>`
     - 
     - The choice that's been exercised on the target contract. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.choice_argument: 

       choice_argument
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - The argument the choice was made with. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.acting_parties: 

       acting_parties
     - :ref:`string <string>`
     - repeated
     - The parties that made the choice. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.consuming: 

       consuming
     - :ref:`bool <bool>`
     - 
     - If true, the target contract may no longer be exercised. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.witness_parties: 

       witness_parties
     - :ref:`string <string>`
     - repeated
     - The parties that are notified of this event. Required
   * - .. _com.digitalasset.ledger.api.v1.ExercisedEvent.child_event_ids: 

       child_event_ids
     - :ref:`string <string>`
     - repeated
     - References to further events in the same transaction that appeared as a result of this ``ExercisedEvent``. It contains only the immediate children of this event, not all members of the subtree rooted at this node. Optional
   








.. _com/digitalasset/ledger/api/v1/ledger_configuration_service.proto:

com/digitalasset/ledger/api/v1/ledger_configuration_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.GetLedgerConfigurationRequest:

GetLedgerConfigurationRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerConfigurationRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerConfigurationRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetLedgerConfigurationResponse:

GetLedgerConfigurationResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerConfigurationResponse.ledger_configuration: 

       ledger_configuration
     - :ref:`LedgerConfiguration <com.digitalasset.ledger.api.v1.LedgerConfiguration>`
     - 
     - The latest ledger configuration.
   



.. _com.digitalasset.ledger.api.v1.LedgerConfiguration:

LedgerConfiguration
===================================================================================================

LedgerConfiguration contains parameters of the ledger instance that may be useful to clients.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.LedgerConfiguration.min_ttl: 

       min_ttl
     -  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Duration>`__
     - 
     - Minimum difference between ledger effective time and maximum record time in submitted commands.
   * - .. _com.digitalasset.ledger.api.v1.LedgerConfiguration.max_ttl: 

       max_ttl
     -  `google.protobuf.Duration <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Duration>`__
     - 
     - Maximum difference between ledger effective time and maximum record time in submitted commands.
   






.. _com.digitalasset.ledger.api.v1.LedgerConfigurationService:

LedgerConfigurationService
===================================================================================================

LedgerConfigurationService allows clients to subscribe to changes of the ledger configuration.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - GetLedgerConfiguration
     - :ref:`GetLedgerConfigurationRequest <com.digitalasset.ledger.api.v1.GetLedgerConfigurationRequest>`
     - :ref:`GetLedgerConfigurationResponse <com.digitalasset.ledger.api.v1.GetLedgerConfigurationResponse>`
     - Returns the latest configuration as the first response, and publishes configuration updates in the same stream.
   



.. _com/digitalasset/ledger/api/v1/ledger_identity_service.proto:

com/digitalasset/ledger/api/v1/ledger_identity_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.GetLedgerIdentityRequest:

GetLedgerIdentityRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerIdentityRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetLedgerIdentityResponse:

GetLedgerIdentityResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerIdentityResponse.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - The ID of the ledger exposed by the server. Requests submitted with the wrong ledger ID will result in ``NOT_FOUND`` gRPC errors. Required
   






.. _com.digitalasset.ledger.api.v1.LedgerIdentityService:

LedgerIdentityService
===================================================================================================

Allows clients to verify that the server they are communicating with exposes the ledger they wish to operate on.
Note that every ledger has a unique ID.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - GetLedgerIdentity
     - :ref:`GetLedgerIdentityRequest <com.digitalasset.ledger.api.v1.GetLedgerIdentityRequest>`
     - :ref:`GetLedgerIdentityResponse <com.digitalasset.ledger.api.v1.GetLedgerIdentityResponse>`
     - Clients may call this RPC to return the identifier of the ledger they are connected to.
   



.. _com/digitalasset/ledger/api/v1/ledger_offset.proto:

com/digitalasset/ledger/api/v1/ledger_offset.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.LedgerOffset:

LedgerOffset
===================================================================================================

Describes a specific point on the ledger.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.LedgerOffset.absolute: 

       absolute
     - :ref:`string <string>`
     - 
     - Absolute values are acquired by reading the transactions in the stream. The offsets can be compared. The format may vary between implementations. It is either a string representing an ever-increasing integer, or a composite string containing ``<block-hash>-<block-height>-<event-id>``; ordering requires comparing numerical values of the second, then the third element.
   * - .. _com.digitalasset.ledger.api.v1.LedgerOffset.boundary: 

       boundary
     - :ref:`LedgerOffset.LedgerBoundary <com.digitalasset.ledger.api.v1.LedgerOffset.LedgerBoundary>`
     - 
     - 
   




.. _com.digitalasset.ledger.api.v1.LedgerOffset.LedgerBoundary:

LedgerOffset.LedgerBoundary
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Name
     - Number
     - Description
   * - .. _com.digitalasset.ledger.api.v1.LedgerOffset.LedgerBoundary.LEDGER_BEGIN:

       LEDGER_BEGIN
     - 0
     - Refers to the first transaction.
   * - .. _com.digitalasset.ledger.api.v1.LedgerOffset.LedgerBoundary.LEDGER_END:

       LEDGER_END
     - 1
     - Refers to the currently last transaction, which is a moving target.
   






.. _com/digitalasset/ledger/api/v1/package_service.proto:

com/digitalasset/ledger/api/v1/package_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.GetPackageRequest:

GetPackageRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetPackageRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetPackageRequest.package_id: 

       package_id
     - :ref:`string <string>`
     - 
     - The ID of the requested package. Required
   * - .. _com.digitalasset.ledger.api.v1.GetPackageRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetPackageResponse:

GetPackageResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetPackageResponse.hash_function: 

       hash_function
     - :ref:`HashFunction <com.digitalasset.ledger.api.v1.HashFunction>`
     - 
     - The hash function we use to calculate the hash. Required
   * - .. _com.digitalasset.ledger.api.v1.GetPackageResponse.archive_payload: 

       archive_payload
     - :ref:`bytes <bytes>`
     - 
     - Contains a ``daml_lf`` ArchivePayload. See further details in ``daml_lf.proto``. Required
   * - .. _com.digitalasset.ledger.api.v1.GetPackageResponse.hash: 

       hash
     - :ref:`string <string>`
     - 
     - The hash of the archive payload, can also used as a ``package_id``. Required
   



.. _com.digitalasset.ledger.api.v1.GetPackageStatusRequest:

GetPackageStatusRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetPackageStatusRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetPackageStatusRequest.package_id: 

       package_id
     - :ref:`string <string>`
     - 
     - The ID of the requested package. Required
   * - .. _com.digitalasset.ledger.api.v1.GetPackageStatusRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetPackageStatusResponse:

GetPackageStatusResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetPackageStatusResponse.package_status: 

       package_status
     - :ref:`PackageStatus <com.digitalasset.ledger.api.v1.PackageStatus>`
     - 
     - The status of the package.
   



.. _com.digitalasset.ledger.api.v1.ListPackagesRequest:

ListPackagesRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.ListPackagesRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.ListPackagesRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.ListPackagesResponse:

ListPackagesResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.ListPackagesResponse.package_ids: 

       package_ids
     - :ref:`string <string>`
     - repeated
     - The IDs of all DAML-LF packages supported by the server. Required
   




.. _com.digitalasset.ledger.api.v1.HashFunction:

HashFunction
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Name
     - Number
     - Description
   * - .. _com.digitalasset.ledger.api.v1.HashFunction.SHA256:

       SHA256
     - 0
     - 
   


.. _com.digitalasset.ledger.api.v1.PackageStatus:

PackageStatus
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Name
     - Number
     - Description
   * - .. _com.digitalasset.ledger.api.v1.PackageStatus.UNKNOWN:

       UNKNOWN
     - 0
     - The server is not aware of such a package.
   * - .. _com.digitalasset.ledger.api.v1.PackageStatus.REGISTERED:

       REGISTERED
     - 1
     - The server is able to execute DAML commands operating on this package.
   




.. _com.digitalasset.ledger.api.v1.PackageService:

PackageService
===================================================================================================

Allows clients to query the DAML-LF packages that are supported by the server.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - ListPackages
     - :ref:`ListPackagesRequest <com.digitalasset.ledger.api.v1.ListPackagesRequest>`
     - :ref:`ListPackagesResponse <com.digitalasset.ledger.api.v1.ListPackagesResponse>`
     - Returns the identifiers of all supported packages.
   * - GetPackage
     - :ref:`GetPackageRequest <com.digitalasset.ledger.api.v1.GetPackageRequest>`
     - :ref:`GetPackageResponse <com.digitalasset.ledger.api.v1.GetPackageResponse>`
     - Returns the contents of a single package, or a ``NOT_FOUND`` error if the requested package is unknown.
   * - GetPackageStatus
     - :ref:`GetPackageStatusRequest <com.digitalasset.ledger.api.v1.GetPackageStatusRequest>`
     - :ref:`GetPackageStatusResponse <com.digitalasset.ledger.api.v1.GetPackageStatusResponse>`
     - Returns the status of a single package.
   



.. _com/digitalasset/ledger/api/v1/testing/reset_service.proto:

com/digitalasset/ledger/api/v1/testing/reset_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.testing.ResetRequest:

ResetRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.testing.ResetRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   






.. _com.digitalasset.ledger.api.v1.testing.ResetService:

ResetService
===================================================================================================

Service to reset the ledger state. The goal here is to be able to reset the state in a way
that's much faster compared to restarting the whole ledger application (be it a sandbox
or the real ledger server).

Note that *all* state present in the ledger implementation will be reset, most importantly
including the ledger ID. This means that clients will have to re-fetch the ledger ID
from the identity service after hitting this endpoint.

The semantics are as follows:

* When the reset service returns the reset is initiated, but not completed;
* While the reset is performed, the ledger will not accept new requests. In fact we guarantee
  that ledger stops accepting new requests by the time the response to Reset is delivered;
* In-flight requests might be aborted, we make no guarantees on when or how quickly this
  happens;
* The ledger might be unavailable for a period of time before the reset is complete.

Given the above, the recommended mode of operation for clients of the reset endpoint is to
call it, then call the ledger identity endpoint in a retry loop that will tolerate a brief
window when the ledger is down, and resume operation as soon as the new ledger ID is delivered.

Note that this service will be available on the sandbox and might be available in some other testing
environments, but will *never* be available in production.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - Reset
     - :ref:`ResetRequest <com.digitalasset.ledger.api.v1.testing.ResetRequest>`
     -  `.google.protobuf.Empty <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Empty>`__
     - Resets the ledger state. Note that loaded DARs won't be removed -- this only rolls back the ledger to genesis.
   



.. _com/digitalasset/ledger/api/v1/testing/time_service.proto:

com/digitalasset/ledger/api/v1/testing/time_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.testing.GetTimeRequest:

GetTimeRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.testing.GetTimeRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   



.. _com.digitalasset.ledger.api.v1.testing.GetTimeResponse:

GetTimeResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.testing.GetTimeResponse.current_time: 

       current_time
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - The current time according to the ledger server.
   



.. _com.digitalasset.ledger.api.v1.testing.SetTimeRequest:

SetTimeRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.testing.SetTimeRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.testing.SetTimeRequest.current_time: 

       current_time
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - MUST precisely match the current time as it's known to the ledger server. On mismatch, an ``INVALID_PARAMETER`` gRPC error will be returned.
   * - .. _com.digitalasset.ledger.api.v1.testing.SetTimeRequest.new_time: 

       new_time
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - The time the client wants to set on the ledger. MUST be a point int time after ``current_time``.
   






.. _com.digitalasset.ledger.api.v1.testing.TimeService:

TimeService
===================================================================================================

Optional service, exposed for testing static time scenarios.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - GetTime
     - :ref:`GetTimeRequest <com.digitalasset.ledger.api.v1.testing.GetTimeRequest>`
     - :ref:`GetTimeResponse <com.digitalasset.ledger.api.v1.testing.GetTimeResponse>`
     - Returns a stream of time updates. Always returns at least one response, where the first one is the current time. Subsequent responses are emitted whenever the ledger server's time is updated.
   * - SetTime
     - :ref:`SetTimeRequest <com.digitalasset.ledger.api.v1.testing.SetTimeRequest>`
     -  `.google.protobuf.Empty <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Empty>`__
     - Allows clients to change the ledger's clock in an atomic get-and-set operation.
   



.. _com/digitalasset/ledger/api/v1/trace_context.proto:

com/digitalasset/ledger/api/v1/trace_context.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.TraceContext:

TraceContext
===================================================================================================

Data structure to propagate Zipkin trace information.
See https://github.com/openzipkin/b3-propagation
Trace identifiers are 64 or 128-bit, but all span identifiers within a trace are 64-bit. All identifiers are opaque.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.TraceContext.trace_id_high: 

       trace_id_high
     - :ref:`uint64 <uint64>`
     - 
     - If present, this is the high 64 bits of the 128-bit identifier. Otherwise the trace ID is 64 bits long.
   * - .. _com.digitalasset.ledger.api.v1.TraceContext.trace_id: 

       trace_id
     - :ref:`uint64 <uint64>`
     - 
     - The TraceId is 64 or 128-bit in length and indicates the overall ID of the trace. Every span in a trace shares this ID.
   * - .. _com.digitalasset.ledger.api.v1.TraceContext.span_id: 

       span_id
     - :ref:`uint64 <uint64>`
     - 
     - The SpanId is 64-bit in length and indicates the position of the current operation in the trace tree. The value should not be interpreted: it may or may not be derived from the value of the TraceId.
   * - .. _com.digitalasset.ledger.api.v1.TraceContext.parent_span_id: 

       parent_span_id
     -  `google.protobuf.UInt64Value <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.UInt64Value>`__
     - 
     - The ParentSpanId is 64-bit in length and indicates the position of the parent operation in the trace tree. When the span is the root of the trace tree, the ParentSpanId is absent.
   * - .. _com.digitalasset.ledger.api.v1.TraceContext.sampled: 

       sampled
     - :ref:`bool <bool>`
     - 
     - When the sampled decision is accept, report this span to the tracing system. When it is reject, do not. When B3 attributes are sent without a sampled decision, the receiver should make one. Once the sampling decision is made, the same value should be consistently sent downstream.
   








.. _com/digitalasset/ledger/api/v1/transaction.proto:

com/digitalasset/ledger/api/v1/transaction.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.Transaction:

Transaction
===================================================================================================

Filtered view of an on-ledger transaction.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Transaction.transaction_id: 

       transaction_id
     - :ref:`string <string>`
     - 
     - Assigned by the server. Useful for correlating logs. Required
   * - .. _com.digitalasset.ledger.api.v1.Transaction.command_id: 

       command_id
     - :ref:`string <string>`
     - 
     - The ID of the command which resulted in this transaction. Missing for everyone except the submitting party. Optional
   * - .. _com.digitalasset.ledger.api.v1.Transaction.workflow_id: 

       workflow_id
     - :ref:`string <string>`
     - 
     - The workflow ID used in command submission. Optional
   * - .. _com.digitalasset.ledger.api.v1.Transaction.effective_at: 

       effective_at
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - Ledger effective time. Required
   * - .. _com.digitalasset.ledger.api.v1.Transaction.events: 

       events
     - :ref:`Event <com.digitalasset.ledger.api.v1.Event>`
     - repeated
     - The collection of events. Only contains ``CreatedEvent`` or ``ArchivedEvent``. Required
   * - .. _com.digitalasset.ledger.api.v1.Transaction.offset: 

       offset
     - :ref:`string <string>`
     - 
     - The absolute offset. The format of this field is described in ``ledger_offset.proto``. Required
   * - .. _com.digitalasset.ledger.api.v1.Transaction.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Zipkin trace context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.TransactionTree:

TransactionTree
===================================================================================================

Complete view of an on-ledger transaction.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.transaction_id: 

       transaction_id
     - :ref:`string <string>`
     - 
     - Assigned by the server. Useful for correlating logs. Required
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.command_id: 

       command_id
     - :ref:`string <string>`
     - 
     - The ID of the command which resulted in this transaction. Missing for everyone except the submitting party. Optional
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.workflow_id: 

       workflow_id
     - :ref:`string <string>`
     - 
     - The workflow ID used in command submission. Only set if the ``workflow_id`` for the command was set. Optional
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.effective_at: 

       effective_at
     -  `google.protobuf.Timestamp <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Timestamp>`__
     - 
     - Ledger effective time. Required
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.offset: 

       offset
     - :ref:`string <string>`
     - 
     - The absolute offset. The format of this field is described in ``ledger_offset.proto``. Required
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.events_by_id: 

       events_by_id
     - :ref:`TransactionTree.EventsByIdEntry <com.digitalasset.ledger.api.v1.TransactionTree.EventsByIdEntry>`
     - repeated
     - Changes to the ledger that were caused by this transaction. Nodes of the transaction tree. Required
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.root_event_ids: 

       root_event_ids
     - :ref:`string <string>`
     - repeated
     - Roots of the transaction tree. Required
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Zipkin trace context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.TransactionTree.EventsByIdEntry:

TransactionTree.EventsByIdEntry
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.EventsByIdEntry.key: 

       key
     - :ref:`string <string>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.TransactionTree.EventsByIdEntry.value: 

       value
     - :ref:`TreeEvent <com.digitalasset.ledger.api.v1.TreeEvent>`
     - 
     - 
   



.. _com.digitalasset.ledger.api.v1.TreeEvent:

TreeEvent
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.TreeEvent.created: 

       created
     - :ref:`CreatedEvent <com.digitalasset.ledger.api.v1.CreatedEvent>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.TreeEvent.exercised: 

       exercised
     - :ref:`ExercisedEvent <com.digitalasset.ledger.api.v1.ExercisedEvent>`
     - 
     - 
   








.. _com/digitalasset/ledger/api/v1/transaction_filter.proto:

com/digitalasset/ledger/api/v1/transaction_filter.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.Filters:

Filters
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Filters.inclusive: 

       inclusive
     - :ref:`InclusiveFilters <com.digitalasset.ledger.api.v1.InclusiveFilters>`
     - 
     - If not set, no filters will be applied. Optional
   



.. _com.digitalasset.ledger.api.v1.InclusiveFilters:

InclusiveFilters
===================================================================================================

If no internal fields are set, no data will be returned.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.InclusiveFilters.template_ids: 

       template_ids
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - repeated
     - A collection of templates. SHOULD NOT contain duplicates. Required
   



.. _com.digitalasset.ledger.api.v1.TransactionFilter:

TransactionFilter
===================================================================================================

Used for filtering Transaction and Active Contract Set streams.
Determines which on-ledger events will be served to the client.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.TransactionFilter.filters_by_party: 

       filters_by_party
     - :ref:`TransactionFilter.FiltersByPartyEntry <com.digitalasset.ledger.api.v1.TransactionFilter.FiltersByPartyEntry>`
     - repeated
     - Keys of the map determine which parties' on-ledger transactions are being queried. Values of the map determine which events are disclosed in the stream per party. At the minimum, a party needs to set an empty Filters message to receive any events. Required
   



.. _com.digitalasset.ledger.api.v1.TransactionFilter.FiltersByPartyEntry:

TransactionFilter.FiltersByPartyEntry
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.TransactionFilter.FiltersByPartyEntry.key: 

       key
     - :ref:`string <string>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.TransactionFilter.FiltersByPartyEntry.value: 

       value
     - :ref:`Filters <com.digitalasset.ledger.api.v1.Filters>`
     - 
     - 
   








.. _com/digitalasset/ledger/api/v1/transaction_service.proto:

com/digitalasset/ledger/api/v1/transaction_service.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.GetLedgerEndRequest:

GetLedgerEndRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerEndRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerEndRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetLedgerEndResponse:

GetLedgerEndResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetLedgerEndResponse.offset: 

       offset
     - :ref:`LedgerOffset <com.digitalasset.ledger.api.v1.LedgerOffset>`
     - 
     - The absolute offset of the current ledger end.
   



.. _com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest:

GetTransactionByEventIdRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest.event_id: 

       event_id
     - :ref:`string <string>`
     - 
     - The ID of a particular event. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest.requesting_parties: 

       requesting_parties
     - :ref:`string <string>`
     - repeated
     - The parties whose events the client expects to see. Events that are not visible for the parties in this collection will not be present in the response. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetTransactionByIdRequest:

GetTransactionByIdRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByIdRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByIdRequest.transaction_id: 

       transaction_id
     - :ref:`string <string>`
     - 
     - The ID of a particular transaction. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByIdRequest.requesting_parties: 

       requesting_parties
     - :ref:`string <string>`
     - repeated
     - The parties whose events the client expects to see. Events that are not visible for the parties in this collection will not be present in the response. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionByIdRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetTransactionResponse:

GetTransactionResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionResponse.transaction: 

       transaction
     - :ref:`TransactionTree <com.digitalasset.ledger.api.v1.TransactionTree>`
     - 
     - 
   



.. _com.digitalasset.ledger.api.v1.GetTransactionTreesResponse:

GetTransactionTreesResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionTreesResponse.transactions: 

       transactions
     - :ref:`TransactionTree <com.digitalasset.ledger.api.v1.TransactionTree>`
     - repeated
     - The list of transaction trees that matches the filter in ``GetTransactionsRequest`` for the ``GetTransactionTrees`` method.
   



.. _com.digitalasset.ledger.api.v1.GetTransactionsRequest:

GetTransactionsRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsRequest.ledger_id: 

       ledger_id
     - :ref:`string <string>`
     - 
     - Must correspond to the ledger ID reported by the Ledger Identification Service. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsRequest.begin: 

       begin
     - :ref:`LedgerOffset <com.digitalasset.ledger.api.v1.LedgerOffset>`
     - 
     - Beginning of the requested ledger section. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsRequest.end: 

       end
     - :ref:`LedgerOffset <com.digitalasset.ledger.api.v1.LedgerOffset>`
     - 
     - End of the requested ledger section. Optional, if not set, the stream will not terminate.
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsRequest.filter: 

       filter
     - :ref:`TransactionFilter <com.digitalasset.ledger.api.v1.TransactionFilter>`
     - 
     - Requesting parties with template filters. Required
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsRequest.verbose: 

       verbose
     - :ref:`bool <bool>`
     - 
     - If enabled, values served over the API will contain more information than strictly necessary to interpret the data. In particular, setting the verbose flag to true triggers the ledger to include labels for record fields. Optional
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsRequest.trace_context: 

       trace_context
     - :ref:`TraceContext <com.digitalasset.ledger.api.v1.TraceContext>`
     - 
     - Server side tracing will be registered as a child of the submitted context. This field is a future extension point and is currently not supported. Optional
   



.. _com.digitalasset.ledger.api.v1.GetTransactionsResponse:

GetTransactionsResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.GetTransactionsResponse.transactions: 

       transactions
     - :ref:`Transaction <com.digitalasset.ledger.api.v1.Transaction>`
     - repeated
     - The list of transactions that matches the filter in GetTransactionsRequest for the GetTransactions method.
   






.. _com.digitalasset.ledger.api.v1.TransactionService:

TransactionService
===================================================================================================

Allows clients to read transactions from the ledger.

.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - GetTransactions
     - :ref:`GetTransactionsRequest <com.digitalasset.ledger.api.v1.GetTransactionsRequest>`
     - :ref:`GetTransactionsResponse <com.digitalasset.ledger.api.v1.GetTransactionsResponse>`
     - Read the ledger's filtered transaction stream for a set of parties.
   * - GetTransactionTrees
     - :ref:`GetTransactionsRequest <com.digitalasset.ledger.api.v1.GetTransactionsRequest>`
     - :ref:`GetTransactionTreesResponse <com.digitalasset.ledger.api.v1.GetTransactionTreesResponse>`
     - Read the ledger's complete transaction stream for a set of parties.
   * - GetTransactionByEventId
     - :ref:`GetTransactionByEventIdRequest <com.digitalasset.ledger.api.v1.GetTransactionByEventIdRequest>`
     - :ref:`GetTransactionResponse <com.digitalasset.ledger.api.v1.GetTransactionResponse>`
     - Lookup a transaction by the ID of an event that appears within it. Returns ``NOT_FOUND`` if no such transaction exists.
   * - GetTransactionById
     - :ref:`GetTransactionByIdRequest <com.digitalasset.ledger.api.v1.GetTransactionByIdRequest>`
     - :ref:`GetTransactionResponse <com.digitalasset.ledger.api.v1.GetTransactionResponse>`
     - Lookup a transaction by its ID. Returns ``NOT_FOUND`` if no such transaction exists.
   * - GetLedgerEnd
     - :ref:`GetLedgerEndRequest <com.digitalasset.ledger.api.v1.GetLedgerEndRequest>`
     - :ref:`GetLedgerEndResponse <com.digitalasset.ledger.api.v1.GetLedgerEndResponse>`
     - Get the current ledger end. Subscriptions started with the returned offset will serve transactions created after this RPC was called.
   



.. _com/digitalasset/ledger/api/v1/value.proto:

com/digitalasset/ledger/api/v1/value.proto
***************************************************************************************************



.. _com.digitalasset.ledger.api.v1.Identifier:

Identifier
===================================================================================================

Unique identifier of an entity.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Identifier.package_id: 

       package_id
     - :ref:`string <string>`
     - 
     - The identifier of the DAML package that contains the entity. Required
   * - .. _com.digitalasset.ledger.api.v1.Identifier.name: 

       name
     - :ref:`string <string>`
     - 
     - The identifier of the entity (unique within the package) DEPRECATED: use ``module_name`` and ``entity_name`` instead Optional
   * - .. _com.digitalasset.ledger.api.v1.Identifier.module_name: 

       module_name
     - :ref:`string <string>`
     - 
     - The dot-separated module name of the identifier. Required
   * - .. _com.digitalasset.ledger.api.v1.Identifier.entity_name: 

       entity_name
     - :ref:`string <string>`
     - 
     - The dot-separated name of the entity (e.g. record, template, ...) within the module. Required
   



.. _com.digitalasset.ledger.api.v1.List:

List
===================================================================================================

A homogenous collection of values.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.List.elements: 

       elements
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - repeated
     - The elements must all be of the same concrete value type. Optional
   



.. _com.digitalasset.ledger.api.v1.Map:

Map
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Map.entries: 

       entries
     - :ref:`Map.Entry <com.digitalasset.ledger.api.v1.Map.Entry>`
     - repeated
     - 
   



.. _com.digitalasset.ledger.api.v1.Map.Entry:

Map.Entry
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Map.Entry.key: 

       key
     - :ref:`string <string>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Map.Entry.value: 

       value
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - 
   



.. _com.digitalasset.ledger.api.v1.Optional:

Optional
===================================================================================================

Corresponds to Java's Optional type, Scala's Option, and Haskell's Maybe.
The reason why we need to wrap this in an additional ``message`` is that we
need to be able to encode the ``None`` case in the ``Value`` oneof.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Optional.value: 

       value
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - optional
   



.. _com.digitalasset.ledger.api.v1.Record:

Record
===================================================================================================

Contains nested values.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Record.record_id: 

       record_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - Omitted from the transaction stream when verbose streaming is not enabled. Optional when submitting commands.
   * - .. _com.digitalasset.ledger.api.v1.Record.fields: 

       fields
     - :ref:`RecordField <com.digitalasset.ledger.api.v1.RecordField>`
     - repeated
     - The nested values of the record. Required
   



.. _com.digitalasset.ledger.api.v1.RecordField:

RecordField
===================================================================================================

A named nested value within a record.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.RecordField.label: 

       label
     - :ref:`string <string>`
     - 
     - Omitted from the transaction stream when verbose streaming is not enabled. If any of the keys are omitted within a single record, the order of fields MUST match the order of declaration in the DAML template. Optional, when submitting commands.
   * - .. _com.digitalasset.ledger.api.v1.RecordField.value: 

       value
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - A nested value of a record. Required
   



.. _com.digitalasset.ledger.api.v1.Value:

Value
===================================================================================================

Encodes values that the ledger accepts as command arguments and emits as contract arguments.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Value.record: 

       record
     - :ref:`Record <com.digitalasset.ledger.api.v1.Record>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Value.variant: 

       variant
     - :ref:`Variant <com.digitalasset.ledger.api.v1.Variant>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Value.contract_id: 

       contract_id
     - :ref:`string <string>`
     - 
     - Identifier of an on-ledger contract. Commands which reference an unknown or already archived contract ID will fail.
   * - .. _com.digitalasset.ledger.api.v1.Value.list: 

       list
     - :ref:`List <com.digitalasset.ledger.api.v1.List>`
     - 
     - Represents a homogenous list of values.
   * - .. _com.digitalasset.ledger.api.v1.Value.int64: 

       int64
     - :ref:`sint64 <sint64>`
     - 
     - 
   * - .. _com.digitalasset.ledger.api.v1.Value.decimal: 

       decimal
     - :ref:`string <string>`
     - 
     - A decimal value with precision 38 (38 decimal digits), of which 10 after the comma / period. in other words a decimal is a number of the form ``x / 10^10`` where ``|x| < 10^38``. The number can start with a leading sign [+-] followed by digits
   * - .. _com.digitalasset.ledger.api.v1.Value.text: 

       text
     - :ref:`string <string>`
     - 
     - A string.
   * - .. _com.digitalasset.ledger.api.v1.Value.timestamp: 

       timestamp
     - :ref:`sfixed64 <sfixed64>`
     - 
     - Microseconds since the UNIX epoch. Can go backwards. Fixed since the vast majority of values will be greater than 2^28, since currently the number of microseconds since the epoch is greater than that. Range: 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999Z, so that we can convert to/from https://www.ietf.org/rfc/rfc3339.txt
   * - .. _com.digitalasset.ledger.api.v1.Value.party: 

       party
     - :ref:`string <string>`
     - 
     - An agent operating on the ledger.
   * - .. _com.digitalasset.ledger.api.v1.Value.bool: 

       bool
     - :ref:`bool <bool>`
     - 
     - True or false.
   * - .. _com.digitalasset.ledger.api.v1.Value.unit: 

       unit
     -  `google.protobuf.Empty <https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Empty>`__
     - 
     - This value is used for example for choices that don't take any arguments.
   * - .. _com.digitalasset.ledger.api.v1.Value.date: 

       date
     - :ref:`int32 <int32>`
     - 
     - Days since the unix epoch. Can go backwards. Limited from 0001-01-01 to 9999-12-31, also to be compatible with https://www.ietf.org/rfc/rfc3339.txt
   * - .. _com.digitalasset.ledger.api.v1.Value.optional: 

       optional
     - :ref:`Optional <com.digitalasset.ledger.api.v1.Optional>`
     - 
     - The Optional type, None or Some
   * - .. _com.digitalasset.ledger.api.v1.Value.map: 

       map
     - :ref:`Map <com.digitalasset.ledger.api.v1.Map>`
     - 
     - The Map type
   



.. _com.digitalasset.ledger.api.v1.Variant:

Variant
===================================================================================================

A value with alternative representations.

.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _com.digitalasset.ledger.api.v1.Variant.variant_id: 

       variant_id
     - :ref:`Identifier <com.digitalasset.ledger.api.v1.Identifier>`
     - 
     - Omitted from the transaction stream when verbose streaming is not enabled. Optional when submitting commands.
   * - .. _com.digitalasset.ledger.api.v1.Variant.constructor: 

       constructor
     - :ref:`string <string>`
     - 
     - Determines which of the Variant's alternatives is encoded in this message. Required
   * - .. _com.digitalasset.ledger.api.v1.Variant.value: 

       value
     - :ref:`Value <com.digitalasset.ledger.api.v1.Value>`
     - 
     - The value encoded within the Variant. Required
   








.. _grpc/health/v1/health_service.proto:

grpc/health/v1/health_service.proto
***************************************************************************************************



.. _grpc.health.v1.HealthCheckRequest:

HealthCheckRequest
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _grpc.health.v1.HealthCheckRequest.service: 

       service
     - :ref:`string <string>`
     - 
     - 
   



.. _grpc.health.v1.HealthCheckResponse:

HealthCheckResponse
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Field
     - Type
     - Label
     - Description
   * - .. _grpc.health.v1.HealthCheckResponse.status: 

       status
     - :ref:`HealthCheckResponse.ServingStatus <grpc.health.v1.HealthCheckResponse.ServingStatus>`
     - 
     - 
   




.. _grpc.health.v1.HealthCheckResponse.ServingStatus:

HealthCheckResponse.ServingStatus
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Name
     - Number
     - Description
   * - .. _grpc.health.v1.HealthCheckResponse.ServingStatus.UNKNOWN:

       UNKNOWN
     - 0
     - 
   * - .. _grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING:

       SERVING
     - 1
     - 
   * - .. _grpc.health.v1.HealthCheckResponse.ServingStatus.NOT_SERVING:

       NOT_SERVING
     - 2
     - 
   




.. _grpc.health.v1.Health:

Health
===================================================================================================



.. list-table::
   :header-rows: 1

   * - Method name
     - Request type
     - Response type
     - Description
   * - Check
     - :ref:`HealthCheckRequest <grpc.health.v1.HealthCheckRequest>`
     - :ref:`HealthCheckResponse <grpc.health.v1.HealthCheckResponse>`
     - 
   



.. _scalarvaluetypes:

Scalar Value Types
***************************************************************************************************

.. list-table::
   :header-rows: 1

   * - .proto type
     - Notes
     - C++ type
     - Java type
     - Python type
   * - .. _double: 

       double
     - 
     - double
     - double
     - float
   * - .. _float: 

       float
     - 
     - float
     - float
     - float
   * - .. _int32: 

       int32
     - Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead.
     - int32
     - int
     - int
   * - .. _int64: 

       int64
     - Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead.
     - int64
     - long
     - int/long
   * - .. _uint32: 

       uint32
     - Uses variable-length encoding.
     - uint32
     - int
     - int/long
   * - .. _uint64: 

       uint64
     - Uses variable-length encoding.
     - uint64
     - long
     - int/long
   * - .. _sint32: 

       sint32
     - Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s.
     - int32
     - int
     - int
   * - .. _sint64: 

       sint64
     - Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s.
     - int64
     - long
     - int/long
   * - .. _fixed32: 

       fixed32
     - Always four bytes. More efficient than uint32 if values are often greater than 2^28.
     - uint32
     - int
     - int
   * - .. _fixed64: 

       fixed64
     - Always eight bytes. More efficient than uint64 if values are often greater than 2^56.
     - uint64
     - long
     - int/long
   * - .. _sfixed32: 

       sfixed32
     - Always four bytes.
     - int32
     - int
     - int
   * - .. _sfixed64: 

       sfixed64
     - Always eight bytes.
     - int64
     - long
     - int/long
   * - .. _bool: 

       bool
     - 
     - bool
     - boolean
     - boolean
   * - .. _string: 

       string
     - A string must always contain UTF-8 encoded or 7-bit ASCII text.
     - string
     - String
     - str/unicode
   * - .. _bytes: 

       bytes
     - May contain any arbitrary sequence of bytes.
     - string
     - ByteString
     - str
   
