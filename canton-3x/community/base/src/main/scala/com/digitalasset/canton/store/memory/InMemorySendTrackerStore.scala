// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.SavePendingSendError.MessageIdAlreadyTracked
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemorySendTrackerStore(implicit executionContext: ExecutionContext)
    extends SendTrackerStore {
  private val pendingSends = new TrieMap[MessageId, CantonTimestamp]()

  override def fetchPendingSends(implicit
      traceContext: TraceContext
  ): Future[Map[MessageId, CantonTimestamp]] =
    Future.successful(pendingSends.toMap)

  override def savePendingSend(messageId: MessageId, maxSequencingTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SavePendingSendError, Unit] = {
    // if putIfAbsent returns a value it means there was an existing item
    pendingSends
      .putIfAbsent(messageId, maxSequencingTime)
      .toLeft(())
      .leftMap(_ => MessageIdAlreadyTracked: SavePendingSendError)
      .toEitherT[Future]
  }

  override def removePendingSend(
      messageId: MessageId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    pendingSends -= messageId
    Future.unit
  }

  override def close(): Unit = ()
}
