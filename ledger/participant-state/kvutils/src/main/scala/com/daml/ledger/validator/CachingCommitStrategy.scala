// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.github.benmanes.caffeine.cache.Cache

import scala.collection.JavaConverters._
import scala.concurrent.Future

class CachingCommitStrategy[Result](
    cache: Cache[DamlStateKey, DamlStateValue],
    delegate: CommitStrategy[Result])
    extends CommitStrategy[Result] {
  override def commit(
      participantId: ParticipantId,
      correlationId: String,
      entryId: DamlKvutils.DamlLogEntryId,
      entry: DamlKvutils.DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue]): Future[Result] = {
    cache.putAll(outputState.asJava)
    delegate.commit(participantId, correlationId, entryId, entry, inputState, outputState)
  }
}
