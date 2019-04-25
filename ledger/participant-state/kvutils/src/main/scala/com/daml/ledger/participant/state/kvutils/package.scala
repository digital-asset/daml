package com.daml.ledger.participant.state

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta,
  Update
}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml_lf.DamlLf.Archive

/** The participant-state key-value utilities provide methods to succintly implement
  * participant-state on top of ledger's that provide a key-value state storage.
  *
  * Key-value utilities are based around the concept of modelling the ledger around
  * an abstract state that can be described (abstractly) as the tuple
  * (List[DamlLogEntryId], Map[DamlLogEntryId, DamlLogEntry], Map[DamlStateKey, DamlStateValue]).
  *
  * The first element describes the ordering of log entries. The second element contains the immutable
  * committed DAML log entries, and the third element describes auxilliary mutable state related to the log entry
  * (e.g. a log entry might describe a DAML transaction containing contracts and the auxilliary mutable data may
  * describe their activeness).
  *
  * While these can be represented in a key-value store directly, some implementations may
  * provide the ordering of log entries from outside the state (e.g. via the transaction chain).
  * The distinction between DAML log entries and DAML state values is that log entries are immutable,
  * and that their keys are not necessarily known beforehand, which is why the implementation talks
  * about them separately.
  *
  * At a high-level the approach for submitting, committing and reading transactions is
  * as follows:
  *
  * 1. The submitted transaction is serialized into a `DamlSubmission` blob, which is
  *    sent to the ledger to be committed. This protocol buffer data structure contains
  *    the transaction and its required inputs for execution.
  *
  * 2. The transaction processor of the underlying ledger receives the `DamlSubmission`,
  *    assigns the submission a unique log entry identifier (this may also be pre-assigned)
  *    and calls `processSubmission` to validate the submission and produce the log entry
  *    that is to be committed, and a list of updates to the key-value state.
  *
  * 3. A read pipeline on participant-state gets a notification of a committed transaction (receives
  *    a new `DamlLogEntryId`), reads the `DamlLogEntry`, and auxilliary data from the state and
  *    produces an `Update` event.
  */
package object kvutils {

  /**
    * Methods to produce a `DamlSubmission`.
    */
  trait KeyValueSubmission {

    def transactionToSubmission(
        submitterInfo: SubmitterInfo,
        meta: TransactionMeta,
        tx: SubmittedTransaction): DamlSubmission

    def archiveToSubmission(archive: Archive): DamlSubmission

    def configurationToSubmission(config: Configuration): DamlSubmission
  }

  /**
    * Methods to process a `DamlSubmission`.
    */
  trait KeyValueCommitting {

    /** Processes a DAML submission, given the allocated log entry id, the submission and its resolved inputs.
      * Produces the log entry to be committed, and DAML state updates.
      *
      * The caller should fetch the inputs declared in `DamlSubmission`.
      *
      * @param engine: The DAML Engine. This instance should be persistent as it caches package compilation.
      * @param config: The ledger configuration.
      * @param entryId: The log entry id to which this submission is committed.
      * @param recordTime: The record time for the log entry.
      * @param submission: The submission to commit to the ledger.
      * @param inputLogEntries: The resolved inputs to the submission.
      * @param inputState: The input DAML state entries.
      * @return The log entry to be committed and the DAML state updates.
      */
    def processSubmission(
        engine: Engine,
        config: Configuration,
        entryId: DamlLogEntryId,
        recordTime: Timestamp,
        submission: DamlSubmission,
        inputLogEntries: Map[DamlLogEntryId, DamlLogEntry],
        inputState: Map[DamlStateKey, DamlStateValue]
    ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue])

  }

  trait KeyValueConsuming {

    /** Construct a participant-state [[Update]] from a [[DamlLogEntry]]
      *
      * @param entryId: The log entry identifier.
      * @param entry: The log entry.
      * @return [[[Update]] constructed from log entry.
      */
    def logEntryToUpdate(entryId: DamlLogEntryId, entry: DamlLogEntry): Update

  }

}
