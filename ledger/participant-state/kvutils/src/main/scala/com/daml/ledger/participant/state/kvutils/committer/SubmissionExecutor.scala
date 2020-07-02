// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission
}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreexecutionResult
import com.daml.ledger.participant.state.kvutils.{DamlStateMap, DamlStateMapWithFingerprints}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time

trait SubmissionExecutor {
  def run(
      entryId: DamlLogEntryId,
      recordTime: Option[Time.Timestamp],
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: DamlStateMap,
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue])

  def dryRun(
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: DamlStateMapWithFingerprints,
  ): PreexecutionResult
}
