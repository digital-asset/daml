// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntryId
import com.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

sealed case class PreExecutionCommitResult[WriteSet](
    successWriteSet: WriteSet,
    outOfTimeBoundsWriteSet: WriteSet,
    involvedParticipants: Set[Ref.ParticipantId],
)

trait PreExecutingCommitStrategy[StateKey, StateValue, ReadSet, WriteSet] {
  def generateReadSet(
      fetchedInputs: Map[StateKey, StateValue],
      accessedKeys: Set[StateKey],
  ): ReadSet

  def generateWriteSets(
      participantId: Ref.ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: Map[StateKey, StateValue],
      preExecutionResult: PreExecutionResult,
  )(implicit executionContext: ExecutionContext): Future[PreExecutionCommitResult[WriteSet]]
}
