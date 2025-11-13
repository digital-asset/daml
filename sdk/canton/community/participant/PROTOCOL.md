Canton Synchronization Protocol
===============================

This document provides an overview of the Canton synchronization protocol and
in particular code pointers to the main implementation classes.

We assume familiarity with the following resources:
- DAML Leger Model: https://docs.daml.com/concepts/ledger-model/index.html
- Canton Overview: https://docs.daml.com/canton/architecture/overview.html#overview-and-assumptions
- Canton 101:
  - Slides: https://docs.google.com/presentation/d/1YbrYSdXR07Iub7hMeU1WdgrrnH9ZUUO7VV_Yjn2NrBo
  - Recording: https://drive.google.com/file/d/10QF46kZMnwQm2suz1dQf9_J51kqDqImI
- Sequencer API, documented in sequencer_service.proto.

Phases of the Protocol
----------------------

The synchronization protocol is used to synchronize across all involved participants the execution of
DAML transactions and synchronizer reassignments.
The protocol creates a "virtual shared ledger";
each participant stores a projection of the virtual shared ledger, called "local ledger".
In this section, the generic structure of the synchronization protocol (independent of transactions and reassignments) is described.
The protocol is organized in 7 phases.

In Phase 1, the submitting participant receives commands (transaction or reassignment) through the `CommandSubmissionService` of the ledger API.
From this, it creates an `MediatorConfirmationRequest`, one or more `EncryptedViewMessage`s, and a `RootHashMessage`.
The submitting participant uses the sequencer to send the `MediatorConfirmationRequest` to a mediator,
the `EncryptedViewMessage`s to those participants that host an informee of the contained view, and
the `RootHashMessage` to the mediator and each informee participant.
The `MediatorConfirmationRequest` informs the mediator about the informees of the request.
The `EncryptedViewMessage` sends the request payload to the informee participants in an encrypted format.
The `RootHashMessage` is an implementation "trick" to establish consensus about the request root hash from Phase 2 onwards;
it allows us to simplify the implementation of the protocol.

In Phase 2, the mediator validates the received `MediatorConfirmationRequest` and `RootHashMessage`.
If the messages are valid, it starts waiting for `ConfirmationResponse`s from a subset of the informee participants,
called "confirming" participants;
otherwise, it rejects the request right away (Phase 5).

In Phase 3, the informee participants decrypt, parse, and validate the received `EncryptedViewMessage`s and `RootHashMessage`, and
produce a `LocalVerdict` (`LocalApprove` or `LocalReject`).
They also lock any contract created or archived by the request to prevent duplicate creations and archivals by concurrent requests.

In Phase 4, each confirming participant sends a `ConfirmationResponse` to the mediator containing the `LocalVerdict` for the request.

In Phase 5, the mediator validates the received confirmation responses and
produces a `MediatorVerdict` (`MediatorApprove`, `MediatorReject`, or `ParticipantReject`).

In Phase 6, the mediator sends a `ConfirmationResultMessage` containing the `MediatorVerdict` to the informee participants.

In Phase 7, the informee participants validate any received `ConfirmationResultMessage` (or the lack thereof).
Based on this validation and the outcome of Phase 3, they commit or rollback the request.
On the ledger API:
- The submitting participant outputs a command completion through the completion service.
- In case of commit, any informee participant outputs an update (transaction or reassignment) through the update service.

In the following, code pointers for the individual phases are provided.

### Phase 1

#### Transaction Commands

The submission of transaction commands through the ledger API is implemented by `ApiCommandSubmissionService.submit`.
The submitting participant uses `Engine.submit` to convert the received commands into a `lf.Transaction` (as well as some metadata),
and then passes it to `CantonSyncService.submitTransaction`.
Next, the `lf.Transaction` is passed to `SynchronizerRouter.submitTransaction`,
which chooses a `ConnectedSynchronizer` suitable for processing the transaction and calls `ConnectedSynchronizer.submitTransaction`.
If the input contracts of the transaction reside on different synchronizers, the `SynchronizerRouter` will submit reassignments
to assign the input contracts to a common synchronizer.
Next, the participant calls `TransactionProcessor.submit`, which produces the `Batch` of messages
(consisting of `InformeeMessage`, which is a subtype of `MediatorConfirmationRequest`, `EncryptedViewMessage`, and `RootHashMessage`)
by using `TransactionProcessingSteps.TrackedTransactionSubmission.prepareBatch`.
Finally, the `Batch` is submitted to the sequencer in `ProtocolProcessor.submitInternal` using `SequencerClientImpl.sendAsync`.

#### Reassignment Commands

The submission of a reassignment command through the ledger API is implemented by `ApiCommandSubmissionService.submitReassignment`.
The participant directly passes the received command to `CantonSyncService.submitReassignment`.
It determines the responsible `ConnectedSynchronizer` (source synchronizer for unassignments and target synchronizer for assignments) and
calls `ConnectedSynchronizer.submitUnassignment` or `ConnectedSynchronizer.submitAssignment`.
Next, the participant calls `(Unassignment|Assignment)Processor.submit`, which produces the `Batch` of messages
by using `(Unassignment|Assignment)ProcessingSteps.createSubmission`.
Finally, the `Batch` is submitted to the sequencer in `ProtocolProcessor.submitInternal` using `SequencerClientImpl.sendAsync`.

### Phase 2

The mediator uses `Mediator.handler` to receive events from the sequencer.
Next, `MediatorEventsProcessor.handle` organizes the events into stages for parallel processing.
The actual validation occurs in `ConfirmationRequestAndResponseProcessor.processRequest`.

### Phase 3

The subscription of the participant to the sequencer is setup in `ConnectedSynchronizer.startAsync`.
In `MessageDispatcher.handleAll`, each participant performs some basic validation and
routes events to the responsible processor (transaction / reassignment / topology / ...).
The actual validation starts with `ProtocolProcessor.processRequest`.
The `LocalVerdict` is computed by `TransactionConfirmationResponsesFactory.createConfirmationResponses` for transaction requests and
`(Unassignment|Assignment)ProcessingSteps.constructPendingDataAndResponse` for transfer requests.

### Phase 4

The `ConfirmationResponse` is sent in `AbstractMessageProcessor.sendResponses` using `SequencerClientImpl.sendAsync`.

### Phase 5

As for Phase 2, the mediator uses `Mediator.handler` and `MediatorEventsProcessor` to receive events from the sequencer and
orchestrate parallel processing.
The validation and aggregation of responses occurs in `ConfirmationRequestAndResponseProcessor.processResponse`.

### Phase 6

The mediator uses `DefaultVerdictSender.sendResult` to send the `ConfirmationResultMessage` to the informee participants.

### Phase 7

As for Phase 3, a participant subscribes to the sequencer in `ConnectedSynchronizer.startAsync` and uses `MessageDispatcher` to route events.
A `ConfirmationResultMessage` is validated in `ProtocolProcessor.processResult`,
which either commits or rolls back the underlying request.
Next, a participant internally provides a stream of `Update`s through `CantonSyncService.stateUpdates`.
In `BatchingParallelIngestionPipe`, the `Update`s are persisted to the DB.
An event containing an `lf.Transaction` is persisted as a sequence of `lf.Node`s.
The command completion service is implemented by `ApiCommandCompletionService.completionStream` and
the ledger update service by `ApiUpdateService.getUpdates`;
these services read the events from the DB, convert them to the ledger API format, and
send them to ledger applications.

Main Data Structures
--------------------

The protocol uses `MerkleTree`s to simultaneously meet privacy and consensus requirements.
In particular, `MerkleTree`s allow for "blinding" those parts of a transaction that a node is not allowed to see.
The term "blinding" means that the blinded data is replaced by its hash;
thus the blinded data cannot be read whilst the root hash of the tree remains unaffected by blinding.

Related to this, `MerkleSeq` is an embedding of a sequence in a Merkle tree that has a space efficient representation,
namely, a sequence with a single unblinded element has size O(log(n)).

### Transaction Processing

At the ledger API, a transaction command is represented as `message Commands` in `commands.proto`.
For the purpose of the synchronization protocol, a sequence of commands is translated to an `lf.Transaction`;
whereas a command consists only of a single node (create / exercise / lookup-by-key),
a transaction also (recursively) contains the descendants of exercise nodes.
The `lf.Transaction` data type is used for representing the local ledger,
but not for synchronization between participants.

The wire format of the synchronization protocol is driven by the following concerns:
1. The descendants of a node can be derived (using `Engine.reinterpret`) from the node itself.
   So the descendants are redundant information.
2. Different nodes of a transaction should be visible to different sets of participants due to privacy requirements.

With Point 1 only, the protocol could send only the root nodes, as every other node is redundant.
As some participant are however not allowed to see a root node (Point 2),
the protocol needs to explicitly send some of the non-root nodes as well.
The `TransactionViewDecomposition` selects those nodes of a transaction, called "views",
that the protocol explicitly sends around.

The `GenTransactionTree` represents an `lf.Transaction`, decomposed into views and embedded in a `MerkleTree`;
this is the main data structure of transaction processing.

A `TransactionView` represents a single view within a `GenTransactionTree`;
it omits any information about ancestor views (e.g. parent nodes) and global metadata.
A `FullTransactionViewTree` also represents a single view within a `GenTransactionTree`;
in contrast to `TransactionView`, it contains information about the position of the view inside of the `GenTransactionTree`
as well as global metadata.
A `LightTransactionViewTree` is a sparse version of `FullTransactionViewTree`, i.e.,
it blinds information about subviews of the represented view.
The `LightTransactionViewTree`s of a request are sent as the payloads of `EncryptedViewMessage`s.

A `FullInformeeTree` is a version of `GenTransactionTree` tailored to mediators.
It blinds all transaction data and keeps only information about informees.
The `FullInformeeTree` is sent as part of an `InformeeMessage`.

### Reassignments Processing

At the ledger API, a reassignment command is specified as `message ReassignmentCommands` in `reassignment_commands.proto`.
Internally, reassignment commands are represented by `participant.state.vx.ReassignmentCommand`.

Unlike for transactions, a participant receives either the entire reassignment command or nothing of it.
Therefore, a reassignment command consists of a single view only.

A `(Unassignment|Assignment)ViewTree` is a reassignment command embedded in a Merkle tree;
this is the main data structure of reassignment processing.

A `Full(Unassignment|Assignment)Tree` is a reassignment command embedded in a Merkle tree, with all nodes unblinded.
This is the data structure used by participants.
It is sent as the payload of an `EncryptedViewMessage`.

A `(Unassignment|Assignment)MediatorMessage` contains a `(Unassignment|Assignment)ViewTree`
where only the metadata required for mediator processing is unblinded.
This is sent to a mediator.
