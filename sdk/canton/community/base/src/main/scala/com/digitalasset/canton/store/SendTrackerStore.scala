// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Keeps track of [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest]]s that have been
  * sent to the sequencer but not yet witnessed.
  */
trait SendTrackerStore extends AutoCloseable {

  /** Fetch all pending sends currently stored. */
  def fetchPendingSends(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[MessageId, CantonTimestamp]]

  /** Saves that a send will be submitted with this message-id and that if sequenced we expect to
    * see a deliver or deliver error by the provided max sequencing time.
    */
  def savePendingSend(messageId: MessageId, maxSequencingTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SavePendingSendError, Unit]

  /** Removes a pending send from the set we are tracking. Implementations should be idempotent and
    * not error if the message-id is not tracked.
    */
  def removePendingSend(messageId: MessageId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]
}

object SendTrackerStore {
  def apply(storage: Storage)(implicit executionContext: ExecutionContext): SendTrackerStore =
    storage match {
      // Always use an in-memory send tracker store, because we block on accessing the send tracker store
      // from the hot loop of the sequencer client.
      case _: MemoryStorage => new InMemorySendTrackerStore()
      case _: DbStorage => new InMemorySendTrackerStore()
    }
}

sealed trait SavePendingSendError
object SavePendingSendError {

  /** The provided message id is already being tracked and cannot be reused until complete */
  case object MessageIdAlreadyTracked extends SavePendingSendError
}
