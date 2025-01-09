// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.SavePendingSendError.MessageIdAlreadyTracked
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class InMemorySendTrackerStore(implicit executionContext: ExecutionContext)
    extends SendTrackerStore {
  private val pendingSends = new TrieMap[MessageId, CantonTimestamp]()

  override def fetchPendingSends(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[MessageId, CantonTimestamp]] =
    FutureUnlessShutdown.pure(pendingSends.toMap)

  override def savePendingSend(messageId: MessageId, maxSequencingTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePendingSendError, Unit] =
    // if putIfAbsent returns a value it means there was an existing item
    pendingSends
      .putIfAbsent(messageId, maxSequencingTime)
      .toLeft(())
      .leftMap(_ => MessageIdAlreadyTracked: SavePendingSendError)
      .toEitherT[FutureUnlessShutdown]

  override def removePendingSend(
      messageId: MessageId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    pendingSends -= messageId
    FutureUnlessShutdown.unit
  }

  override def close(): Unit = ()
}
