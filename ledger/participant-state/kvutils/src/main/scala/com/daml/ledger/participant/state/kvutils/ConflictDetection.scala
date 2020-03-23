package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}

object ConflictDetection {

  /** Detect conflicts in a log entry and attempt to recover. */
  def conflictDetectAndRecover(
      invalidatedKeys: Set[DamlStateKey],
      inputKeys: Set[DamlStateKey],
      logEntry: DamlLogEntry,
      outputState: Map[DamlStateKey, DamlStateValue])
    : Option[(DamlLogEntry, Map[DamlStateKey, DamlStateValue])] = {

    val conflicts = inputKeys.exists(invalidatedKeys.contains) ||
      outputState.keySet.exists(invalidatedKeys.contains)

    if (!conflicts) {
      // No conflict keys, nothing to change.
      Some((logEntry, outputState))
    } else {
      // Conflicting keys. Produce an appropriate rejection or modify the result.

      logEntry.getPayloadCase match {
        case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
          val txEntry = logEntry.getTransactionEntry

          val builder = DamlLogEntry.newBuilder
          builder.setRecordTime(logEntry.getRecordTime)
          builder.getTransactionRejectionEntryBuilder
            .setSubmitterInfo(txEntry.getSubmitterInfo)
            .getInconsistentBuilder
            .setDetails("Conflict detected") // FIXME what to report

          // FIXME(JM): How to handle command deduplication? This is retryable,
          // but should be retryable with the same command id?
          Some((builder.build, Map.empty))

        case DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
          // TODO(JM): There's two ways this could fail:
          // duplicate submission id or some packages were already uploaded
          // by a submission in the same batch. We could look at the conflicting
          // keys to distinguish the cases and in the latter case just drop the
          // packages from the log entry and output state. This is likely overkill
          // though, so instead we'll just completely drop the submission.
          // Note though that some test-tool tests fail because of this.
          None

        case DamlLogEntry.PayloadCase.PARTY_ALLOCATION_ENTRY =>
          // TODO(JM): Does it already exist or is it a duplicate? Same reasoning
          // applies here as with package upload.
          None

        case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
          // TODO(JM): Either duplicate, or there's a concurrent configuration change.
          // Very rare so fine to drop this.
          None

        case other =>
          sys.error(s"conflictDetectAndRecover: Unhandled case: $other")

      }
    }

  }

}
