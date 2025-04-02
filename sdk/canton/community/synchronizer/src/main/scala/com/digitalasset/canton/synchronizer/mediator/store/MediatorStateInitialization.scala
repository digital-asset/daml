// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicReference

private[mediator] trait MediatorStateInitialization { self: NamedLogging =>
  private val initialized: AtomicReference[MediatorStateInitialization.State] =
    new AtomicReference[MediatorStateInitialization.State](
      MediatorStateInitialization.State.Uninitialized
    )

  /** Clients must call this method before any other method.
    *
    * @param firstEventTs
    *   the timestamp used to subscribe to the sequencer, i.e., all data with a requestTime greater
    *   than or equal to `firstEventTs` will be deleted so that sequencer events can be replayed
    */
  final def initialize(firstEventTs: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    require(
      initialized.compareAndSet(
        MediatorStateInitialization.State.Uninitialized,
        MediatorStateInitialization.State.Initializing,
      ),
      "The store most not be initialized more than once!",
    )
    implicit val directExecutionContext: DirectExecutionContext = DirectExecutionContext(logger)
    doInitialize(firstEventTs).map(_ =>
      initialized.set(MediatorStateInitialization.State.Initialized)
    )
  }

  /** Populate in-memory caches and delete all data with `requestTime` greater than or equal to
    * `deleteFromInclusive`.
    */
  protected def doInitialize(deleteFromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  protected final def requireInitialized()(implicit traceContext: TraceContext): Unit =
    ErrorUtil.requireState(
      initialized.get() == MediatorStateInitialization.State.Initialized,
      "The initialize method needs to be called first.",
    )
}

private[mediator] object MediatorStateInitialization {
  private[mediator] sealed trait State extends Product with Serializable
  private[mediator] object State {
    case object Uninitialized extends State
    case object Initializing extends State
    case object Initialized extends State
  }
}
