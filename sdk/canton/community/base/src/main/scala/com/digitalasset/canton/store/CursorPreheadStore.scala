// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.data.{CantonTimestamp, Counter}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounterDiscriminator, SequencerCounterDiscriminator}

import scala.concurrent.{ExecutionContext, Future}

/** Storage for a cursor prehead. */
trait CursorPreheadStore[Discr] extends AutoCloseable {
  private[store] implicit def ec: ExecutionContext

  /** Gets the prehead of the cursor. */
  def prehead(implicit traceContext: TraceContext): Future[Option[CursorPrehead[Discr]]]

  /** Forces an update to the cursor prehead. Only use this for maintenance and testing. */
  // Cannot implement this method with advancePreheadTo/rewindPreheadTo
  // because it allows to atomically overwrite the timestamp associated with the current prehead.
  private[canton] def overridePreheadUnsafe(newPrehead: Option[CursorPrehead[Discr]])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Sets the prehead counter to `newPrehead` unless it is already at the same or a higher value.
    * The prehead counter should be set to the counter before the head of the corresponding cursor.
    */
  def advancePreheadTo(newPrehead: CursorPrehead[Discr])(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Unit] =
    advancePreheadToTransactionalStoreUpdate(newPrehead).runStandalone()

  /** [[advancePreheadTo]] as a [[com.digitalasset.canton.resource.TransactionalStoreUpdate]] */
  def advancePreheadToTransactionalStoreUpdate(newPrehead: CursorPrehead[Discr])(implicit
      traceContext: TraceContext
  ): TransactionalStoreUpdate

  /** Sets the prehead counter to `newPreheadO` if it is currently set to a higher value. */
  def rewindPreheadTo(newPreheadO: Option[CursorPrehead[Discr]])(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

/** Information for the prehead of a cursor.
  * The prehead of a cursor is the counter before the cursors' head, if any.
  *
  * @param counter The counter corresponding to the prehead
  * @param timestamp The timestamp corresponding to the prehead
  */
final case class CursorPrehead[Discr](counter: Counter[Discr], timestamp: CantonTimestamp)
    extends PrettyPrinting {

  override def pretty: Pretty[CursorPrehead.this.type] = prettyOfClass(
    param("counter", _.counter),
    param("timestamp", _.timestamp),
  )
}

object CursorPrehead {
  type SequencerCounterCursorPrehead = CursorPrehead[SequencerCounterDiscriminator]
  type RequestCounterCursorPrehead = CursorPrehead[RequestCounterDiscriminator]
}
