package com.daml.ledger.validator.preexecution

import java.time.Instant

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet

sealed case class PreExecutionOutput[WriteSet](
    minRecordTime: Option[Instant],
    maxRecordTime: Option[Instant],
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    readSet: ReadSet,
    involvedParticipants: Set[ParticipantId]
)
