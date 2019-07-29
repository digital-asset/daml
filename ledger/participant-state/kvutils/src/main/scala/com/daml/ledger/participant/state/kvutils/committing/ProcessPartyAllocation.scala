package com.daml.ledger.participant.state.kvutils.committing

import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.prettyEntryId
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory

case class ProcessPartyAllocation(
    entryId: DamlLogEntryId,
    recordTime: Timestamp,
    partyAllocationEntry: DamlPartyAllocationEntry,
    inputState: Map[DamlStateKey, Option[DamlStateValue]]
) {
  import Common._

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    (for {
      // 1. Verify that the party isn't empty
      _ <- checkPartyValidity

      // 2. Verify that this is not a duplicate party submission.
      partyKey = DamlStateKey.newBuilder.setParty(party).build
      _ <- deduplicate(partyKey)

      // Build the new log entry and state.
      logEntry = DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setPartyAllocationEntry(partyAllocationEntry)
        .build
      newState = Map(
        partyKey -> DamlStateValue.newBuilder
          .setParty(
            DamlPartyAllocation.newBuilder
              .setParticipantId(partyAllocationEntry.getParticipantId)
          )
          .build)

    } yield (logEntry, newState)).fold((_, Map.empty), identity)

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val submissionId = partyAllocationEntry.getSubmissionId
  private val party: String = partyAllocationEntry.getParty

  private def tracelog(msg: String): Unit =
    logger.trace(
      s"processPartyAllocation[entryId=${prettyEntryId(entryId)}, submId=$submissionId]: $msg")

  private def checkPartyValidity: CheckResult =
    if (party.isEmpty) {
      tracelog(s"Party: $party allocation failed, party string invalid.")
      reject {
        _.setInvalidName(
          DamlPartyAllocationRejectionEntry.InvalidName.newBuilder
            .setDetails(s"Party string '$party' invalid"))
      }
    } else {
      pass()
    }

  private def deduplicate(partyKey: DamlStateKey): CheckResult = {
    if (inputState(partyKey).isEmpty) {
      tracelog(s"Party: $party allocation committed.")
      pass()
    } else {
      reject {
        _.setAlreadyExists(
          DamlPartyAllocationRejectionEntry.AlreadyExists.newBuilder.setDetails(""))
      }
    }
  }

  private def reject(
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder
  ): CheckResult =
    Left(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setPartyAllocationRejectionEntry(
          addErrorDetails(
            DamlPartyAllocationRejectionEntry.newBuilder
              .setSubmissionId(partyAllocationEntry.getSubmissionId)
              .setParticipantId(partyAllocationEntry.getParticipantId)
          )
        )
        .build
    )

}
