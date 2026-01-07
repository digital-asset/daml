// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.SavePendingSendError.MessageIdAlreadyTracked
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap

class InMemorySendTrackerStore() extends SendTrackerStore {
  private val pendingSends = new TrieMap[MessageId, CantonTimestamp]()

  override def fetchPendingSends(implicit
      traceContext: TraceContext
  ): Map[MessageId, CantonTimestamp] = pendingSends.toMap

  override def savePendingSend(messageId: MessageId, maxSequencingTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Either[SavePendingSendError, Unit] =
    // if putIfAbsent returns a value it means there was an existing item
    pendingSends
      .putIfAbsent(messageId, maxSequencingTime)
      .toLeft(())
      .leftMap(_ => MessageIdAlreadyTracked: SavePendingSendError)

  override def removePendingSend(
      messageId: MessageId
  )(implicit traceContext: TraceContext): Unit =
    pendingSends -= messageId

  override def close(): Unit = ()
}
