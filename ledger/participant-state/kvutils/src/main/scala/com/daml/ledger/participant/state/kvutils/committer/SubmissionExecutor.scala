// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission,
}
import com.daml.ledger.participant.state.kvutils.DamlStateMap
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext

trait SubmissionExecutor {
  def run(
      recordTime: Option[Time.Timestamp],
      submission: DamlSubmission,
      participantId: Ref.ParticipantId,
      inputState: DamlStateMap,
  )(implicit loggingContext: LoggingContext): (DamlLogEntry, Map[DamlStateKey, DamlStateValue])

  def runWithPreExecution(
      submission: DamlSubmission,
      participantId: Ref.ParticipantId,
      inputState: DamlStateMap,
  )(implicit loggingContext: LoggingContext): PreExecutionResult
}
