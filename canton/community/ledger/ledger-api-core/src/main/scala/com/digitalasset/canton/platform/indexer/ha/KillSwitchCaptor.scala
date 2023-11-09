// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import org.apache.pekko.stream.KillSwitch
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference

/** This KillSwitch captures it's usage in it's internal state, which can be queried.
  * Captured state is available with the 'state' method.
  *
  * Rules of state transitions:
  * - Shutdown is always the final state
  * - From multiple aborts, the last abort wins
  *
  * With setDelegate() we can set a delegate KillSwitch, which to usage will be replayed
  */
class KillSwitchCaptor(val loggerFactory: NamedLoggerFactory)(implicit traceContext: TraceContext)
    extends KillSwitch
    with NamedLogging {
  import KillSwitchCaptor.*
  import State.*

  private val _state = new AtomicReference[State](Unused)
  private val _delegate = new AtomicReference[Option[KillSwitch]](None)

  private def updateState(newState: Used): Unit = {
    _state.getAndAccumulate(
      newState,
      {
        case (Shutdown, _) => Shutdown
        case (_, used) => used
      },
    )
    ()
  }

  override def shutdown(): Unit = {
    logger.info("Shutdown called!")
    updateState(Shutdown)
    _delegate.get.foreach { ks =>
      logger.info("Shutdown call delegated!")
      ks.shutdown()
    }
  }

  override def abort(ex: Throwable): Unit = {
    logger.info(s"Abort called! (${ex.getMessage})")
    updateState(Aborted(ex))
    _delegate.get.foreach { ks =>
      logger.info(s"Abort call delegated! (${ex.getMessage})")
      ks.abort(ex)
    }
  }

  def state: State = _state.get()
  def setDelegate(delegate: Option[KillSwitch]): Unit = _delegate.set(delegate)
}

object KillSwitchCaptor {
  sealed trait State
  object State {
    case object Unused extends State
    sealed trait Used extends State
    case object Shutdown extends Used
    final case class Aborted(t: Throwable) extends Used
  }
}
