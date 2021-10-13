// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator
import com.daml.ledger.participant.state.kvutils.store.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Determines how we commit the results of processing a Daml submission.
  *
  * This must write deterministically. The output and order of writes should not vary with the same
  * input, even across process runs. This also means that the implementing type must not depend on
  * the order of the `inputState` and `outputState` maps.
  */
trait CommitStrategy[Result] {
  def commit(
      participantId: Ref.ParticipantId,
      correlationId: String,
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue],
      exporterWriteSet: Option[SubmissionAggregator.WriteSetBuilder] = None,
  )(implicit loggingContext: LoggingContext): Future[Result]
}
