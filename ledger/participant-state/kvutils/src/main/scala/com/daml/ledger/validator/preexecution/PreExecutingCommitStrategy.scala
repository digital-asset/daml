// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.v1.ParticipantId

import scala.concurrent.{ExecutionContext, Future}

sealed case class PreExecutionCommitResult[WriteSet](
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    involvedParticipants: Set[ParticipantId]
)

trait PreExecutingCommitStrategy[StateKey, StateValue, ReadSet, WriteSet] {
  def generateReadSet(
      fetchedInputs: Map[StateKey, StateValue],
      accessedKeys: Set[StateKey],
  ): ReadSet

  def generateWriteSets(
      participantId: ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: Map[StateKey, StateValue],
      preExecutionResult: PreExecutionResult,
  )(implicit executionContext: ExecutionContext): Future[PreExecutionCommitResult[WriteSet]]
}
