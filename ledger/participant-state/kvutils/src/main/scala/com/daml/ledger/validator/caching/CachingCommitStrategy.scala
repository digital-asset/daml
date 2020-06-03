// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.CommitStrategy

import scala.concurrent.{ExecutionContext, Future}

class CachingCommitStrategy[Result](
    cache: Cache[DamlStateKey, DamlStateValue],
    shouldCache: DamlStateKey => Boolean,
    delegate: CommitStrategy[Result])(implicit executionContext: ExecutionContext)
    extends CommitStrategy[Result] {
  override def commit(
      participantId: ParticipantId,
      correlationId: String,
      entryId: DamlKvutils.DamlLogEntryId,
      entry: DamlKvutils.DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue]): Future[Result] =
    for {
      _ <- Future {
        outputState.view.filter { case (key, _) => shouldCache(key) }.foreach {
          case (key, value) => cache.put(key, value)
        }
      }
      result <- delegate.commit(
        participantId,
        correlationId,
        entryId,
        entry,
        inputState,
        outputState)
    } yield result
}
