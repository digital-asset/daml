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
  import Commit._

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val submissionId = partyAllocationEntry.getSubmissionId
  private val party: String = partyAllocationEntry.getParty
  val partyKey = DamlStateKey.newBuilder.setParty(party).build

  private def tracelog(msg: String): Unit =
    logger.trace(s"[entryId=${prettyEntryId(entryId)}, submId=$submissionId]: $msg")

  def run: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    Commit.run(
      sequence(
        validateParty,
        deduplicate,
        buildFinalResult
      )
    )

  private val buildFinalResult: Commit[Unit] = delay {
    done(
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setPartyAllocationEntry(partyAllocationEntry)
        .build
    )
  }

  private val validateParty: Commit[Unit] = delay {
    if (party.isEmpty) {
      tracelog(s"Party: $party allocation failed, party string invalid.")
      reject {
        _.setInvalidName(
          DamlPartyAllocationRejectionEntry.InvalidName.newBuilder
            .setDetails(s"Party string '$party' invalid"))
      }
    } else {
      pass
    }
  }

  private val deduplicate: Commit[Unit] = delay {
    if (inputState(partyKey).isEmpty) {
      tracelog(s"Party: $party allocation committed.")
      set(
        partyKey -> DamlStateValue.newBuilder
          .setParty(
            DamlPartyAllocation.newBuilder
              .setParticipantId(partyAllocationEntry.getParticipantId)
          )
          .build
      )
    } else {
      reject {
        _.setAlreadyExists(
          DamlPartyAllocationRejectionEntry.AlreadyExists.newBuilder.setDetails(""))
      }
    }
  }

  private def reject(
      addErrorDetails: DamlPartyAllocationRejectionEntry.Builder => DamlPartyAllocationRejectionEntry.Builder
  ): Commit[Unit] =
    done(
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
