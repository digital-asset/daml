// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state


/** The participant-state key-value utilities provide methods to succintly implement
  * [[com.daml.ledger.participant.state.v1.ReadService]] and
  * [[com.daml.ledger.participant.state.v1.WriteService]] on top of ledger's that provide a key-value state storage.
  *
  * The key-value utilities are based around the concept of modelling the ledger around
  * an abstract state that can be described as the tuple `(logEntryIds, logEntryMap, kvState)`,
  * of type `(List[DamlLogEntryId], Map[DamlLogEntryId, DamlLogEntry], Map[DamlStateKey, DamlStateValue])`.
  *
  * `logEntryIds` describes the ordering of log entries. The `logEntryMap` contains the data for the log entries.
  * This map is expected to be append-only and existing entries are never modified or removed.
  * `kvState` describes auxilliary mutable state which may be created as part of one log entry and mutated by a later one.
  * (e.g. a log entry might describe a DAML transaction containing contracts and the auxilliary mutable data may
  * describe their activeness).
  *
  * While these can be represented in a key-value store directly, some implementations may
  * provide the ordering of log entries from outside the state (e.g. via a transaction chain).
  * The distinction between DAML log entries and DAML state values is that log entries are immutable,
  * and that their keys are not necessarily known beforehand, which is why the implementation deals
  * with them separately, even though both log entries and DAML state values may live in the same storage.
  *
  * As an example, lets look at how a DAML transaction is committed in the [[InMemoryKVParticipantState]].
  * The example implementation maintains a bytes to bytes key-value store and a linked-list of committed
  * blocks. The log entries and the DAML state is stored in the same key-value store and the linked-list of
  * commits provides the ordering to the log entries. Log entry identifiers are randomly generated.
  *
  * The transaction submission with the in-memory example would look as follows:
  *
  * 1. The [[InMemoryKVParticipantState.submitTransaction]] is invoked in the DAML Ledger API server.
  *  The implementation assigns a unique log entry identifier "D8AF41AB" for the submission and using [[KeyValueSubmission]]
  *  produces a [[DamlKvutils.DamlSubmission]] which is then asynchronously submitted for processing.
  *
  * 2. The commit actor receives the submission, verifies that the identifier for the log entry
  *  is unique, fetches the inputs to the submission and calls [[KeyValueCommitting.processSubmission]] to
  *  validate and produce the log entry and state updates. The log entry is stored on the ledger in the
  *  location assigned at submission, and the state updates are applied. The submission itself and the
  *  log entry identifier is appended to the linked-list of commits.
  *
  *  The applied state updates:
  *
  *    * `DS/DamlStateKey(command_dedup = { command_id = ... }) = {}`
  *    * `DS/DamlStateKey(contract_id = (D8AF41AB, 0)) = DamlContractState(active_at = 2019-04-29...)`
  *    * `L/DamlLogEntryId(D8AF41AB) = DamlLogEntry(transaction_entry = ...)`
  *
  * 3. The [[InMemoryKVParticipant.stateUpdates]] emits a new update event from the new state of the ledger
  *  by retrieving the [[DamlLogEntry]] pointed to by the latest commit and transforms it into [[v1.Update]]
  *  using [[KeyValueConsumption.logEntryToUpdate]], and pairs it with an offset corresponding to the
  *  position of the commit (the "block height" if you will).
  *
  * The above example implementation is suitable for situations where the inputs to the transaction have
  * to be pre-computed. For ledger's that do not require this a more natural implementation could assign
  * the log entry identifiers at commit time and store the log entries with a monotonically increasing
  * integer as the key (which would allow for efficient range queries).
  */
package object kvutils {}
