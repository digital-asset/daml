// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.FiniteDuration

/** Manages cancellable timeouts on behalf of another module; it is parametric in the type of the timeout message
  * to send to the owning module and in the type of the handle that represents a cancellable timeout.
  */
class TimeoutManager[E <: Env[E], ParentModuleMessageT, TimeoutIdT](
    override val loggerFactory: NamedLoggerFactory,
    timeout: FiniteDuration,
    timeoutId: TimeoutIdT,
) extends NamedLogging {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var timeoutCancellable: Option[CancellableEvent] = None

  def scheduleTimeout[TimeoutMessageT <: ParentModuleMessageT](
      timeoutEvent: TimeoutMessageT
  )(implicit
      context: E#ActorContextT[ParentModuleMessageT],
      traceContext: TraceContext,
  ): Unit = {
    val cancellableEvent = context.delayedEvent(timeout, timeoutEvent)
    timeoutCancellable match {
      case Some(previousTimeout) =>
        previousTimeout.cancel().discard
        logger.debug(
          s"Rescheduling timeout w/ duration: $timeout; previous event: $previousTimeout; new event: $timeoutEvent"
        )
      case None =>
        logger.debug(
          s"Scheduling new timeout w/ duration: $timeout; new event: $timeoutEvent"
        )
    }
    timeoutCancellable = Some(cancellableEvent)
  }

  def cancelTimeout()(implicit traceContext: TraceContext): Unit = {
    timeoutCancellable.foreach(_.cancel().discard)
    timeoutCancellable = None
    logger.debug(s"Canceling timeout w/ ID: $timeoutId")
  }
}
