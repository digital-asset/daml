// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.util.concurrent.atomic.AtomicReference

import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}

/** This KillSwitch captures it's usage in it's internal state, which can be queried.
  * Captured state is available with the 'state' method.
  *
  * Rules of state transitions:
  * - Shutdown is always the final state
  * - From multiple aborts, the last abort wins
  *
  * With setDelegate() we can set a delegate KillSwitch, which to usage will be replayed
  */
class KillSwitchCaptor(implicit loggingContext: LoggingContext) extends KillSwitch {
  import KillSwitchCaptor._
  import State._

  private val logger = ContextualizedLogger.get(this.getClass)

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
