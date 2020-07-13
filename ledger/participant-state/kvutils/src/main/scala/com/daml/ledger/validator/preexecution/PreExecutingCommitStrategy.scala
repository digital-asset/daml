package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.Key

import scala.concurrent.Future

sealed case class PreExecutionCommitResult[WriteSet](
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    involvedParticipants: Set[ParticipantId]
)

object PreExecutionCommitResult {
  type ReadSet = Seq[(Key, Fingerprint)]
}

trait PreExecutingCommitStrategy[WriteSet] {
  def generateWriteSets(
      participantId: ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      preExecutionResult: PreExecutionResult): Future[PreExecutionCommitResult[WriteSet]]
}
