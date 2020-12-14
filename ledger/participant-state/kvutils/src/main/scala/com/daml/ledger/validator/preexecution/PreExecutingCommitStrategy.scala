// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.{DamlStateMapWithFingerprints, Fingerprint}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.Key

import scala.concurrent.{ExecutionContext, Future}

sealed case class PreExecutionCommitResult[WriteSet](
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    involvedParticipants: Set[ParticipantId]
)

object PreExecutionCommitResult {
  type ReadSet = Seq[(Key, Fingerprint)]
}

trait PreExecutingCommitStrategy[ReadSet, WriteSet] {
  def generateReadSet(
      fetchedInputs: DamlStateMapWithFingerprints,
      accessedKeys: Set[DamlStateKey],
  ): ReadSet

  def generateWriteSets(
      participantId: ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      preExecutionResult: PreExecutionResult,
  )(implicit executionContext: ExecutionContext): Future[PreExecutionCommitResult[WriteSet]]
}
