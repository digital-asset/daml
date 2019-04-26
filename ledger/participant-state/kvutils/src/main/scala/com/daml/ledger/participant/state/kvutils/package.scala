package com.daml.ledger.participant.state

package object kvutils {

  /** The participant-state key-value utilities provide methods to succintly implement
  * [[com.daml.ledger.participant.state.v1.ReadService]] and
  * [[com.daml.ledger.participant.state.v1.WriteService]] on top of ledger's that provide a key-value state storage.
  *
  * The key-value utilities are based around the concept of modelling the ledger around
  * an abstract state that can be described (abstractly) as the tuple (logEntryIds, logEntryMap, kvState),
  * of type (List[DamlLogEntryId], Map[DamlLogEntryId, DamlLogEntry], Map[DamlStateKey, DamlStateValue]).
  *
  * `logEntryIds` describes the ordering of log entries. The `logEntryMap` contains the data for the log entries.
  * This map is expected to be append-only and existing entries are never modified or removed.
  * `kvState` describes auxilliary mutable state which may be created as part of one log entry and mutated by a later one.
  * (e.g. a log entry might describe a DAML transaction containing contracts and the auxilliary mutable data may
  * describe their activeness).
  *
  * While these can be represented in a key-value store directly, some implementations may
  * provide the ordering of log entries from outside the state (e.g. via the transaction chain).
  * The distinction between DAML log entries and DAML state values is that log entries are immutable,
  * and that their keys are not necessarily known beforehand, which is why the implementation deals
  * with them separately.
  *
  * As an example, lets take an implementation in which the ledger deals with an ordered chain of transactions
  * that, when applied, mutate a key-value store. In this setup we can choose to allocate entry identifiers
  * using a random number generator and validating uniqueness at commit time. We furthermore assume that
  * the log entry identifier can be derived from the transaction entry of the underlying ledger.
  *
  * With these assumptions an example submission looks as follows:
  *
  * 1. The [[v1.WriteService.submitTransaction]] is invoked in the DAML Ledger API server.
  *  The implementation assigns a unique log entry identifier "D8AF41AB" for the submission and using [[KeyValueSubmission]]
  *  produces a [[DamlKvutils.DamlSubmission]] which is then communicated to the underlying ledger for committing.
  *
  * 2. The underlying ledger's transaction processor receives the submission, verifies that the identifier for the log entry
  *  is unique, fetches the inputs to the submission and calls [[KeyValueCommitting.processSubmission]] to
  *  validate and produce the log entry and state updates. The log entry is stored on the ledger in the
  *  location assigned at submission, and the state updates are applied.
  *
  *  State after applying changes:
  *    damlState/DamlStateKey(command_dedup = <...>) = ()
  *    damlState/DamlStateKey(contract_id = (D8AF41AB, 1) = DamlContractState(active_at = <...>)
  *    damlLogEntries/D8AF41AB = DamlLogEntry(transaction_entry = <...>)
  *
  * 3. The implementation of [[v1.ReadService.stateUpdates]] receives event on newly committed ledger entry.
  *  From the ledger's transaction it deduces the unique log entry identifier "D8AF41AB" and queries the key-value
  *  store to retrieve key `damlLogEntries/D8AF41AB`, and using [[KeyValueConsumption.logEntryToUpdate]] produces
  *  an [[v1.Update]], with an offset corresponding to the block height of the ledger (for example).
  */

}
