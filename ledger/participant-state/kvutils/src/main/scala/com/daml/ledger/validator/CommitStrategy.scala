// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator
import com.daml.ledger.participant.state.v1.ParticipantId

import scala.concurrent.Future

/**
  * Determines how we commit the results of processing a DAML submission.
  *
  * This must write deterministically. The output and order of writes should not vary with the same
  * input, even across process runs. This also means that the implementing type must not depend on
  * the order of the `inputState` and `outputState` maps.
  */
trait CommitStrategy[Result] {
  def commit(
      participantId: ParticipantId,
      correlationId: String,
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue],
      exporterWriteSet: Option[SubmissionAggregator.WriteSetBuilder] = None,
  ): Future[Result]
}
