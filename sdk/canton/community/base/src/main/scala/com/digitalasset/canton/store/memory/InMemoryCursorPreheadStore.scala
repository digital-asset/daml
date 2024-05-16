// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.store.{CursorPrehead, CursorPreheadStore}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InMemoryCursorPreheadStore[Discr](protected val loggerFactory: NamedLoggerFactory)
    extends CursorPreheadStore[Discr]
    with NamedLogging {

  override private[store] implicit val ec: ExecutionContext =
    DirectExecutionContext(noTracingLogger)

  private val preheadRef = new AtomicReference[Option[CursorPrehead[Discr]]](None)

  override def prehead(implicit
      traceContext: TraceContext
  ): Future[Option[CursorPrehead[Discr]]] =
    Future.successful(preheadRef.get())

  @VisibleForTesting
  private[canton] override def overridePreheadUnsafe(newPrehead: Option[CursorPrehead[Discr]])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(preheadRef.set(newPrehead))

  override def advancePreheadToTransactionalStoreUpdate(
      newPrehead: CursorPrehead[Discr]
  )(implicit traceContext: TraceContext): TransactionalStoreUpdate =
    TransactionalStoreUpdate.InMemoryTransactionalStoreUpdate {
      val _ = preheadRef.getAndUpdate {
        case None => Some(newPrehead)
        case old @ Some(oldPrehead) =>
          if (oldPrehead.counter < newPrehead.counter) Some(newPrehead) else old
      }
    }

  override def rewindPreheadTo(
      newPreheadO: Option[CursorPrehead[Discr]]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info(s"Rewinding prehead to $newPreheadO")
    newPreheadO match {
      case None => preheadRef.set(None)
      case Some(newPrehead) =>
        val _ = preheadRef.getAndUpdate {
          case None => None
          case old @ Some(oldPrehead) =>
            if (oldPrehead.counter > newPrehead.counter) Some(newPrehead) else old
        }
    }
    Future.unit
  }

  override def close(): Unit = ()
}
