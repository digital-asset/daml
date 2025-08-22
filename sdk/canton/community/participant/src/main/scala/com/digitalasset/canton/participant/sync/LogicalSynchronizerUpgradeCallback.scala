// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.data.SynchronizerSuccessor
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

trait LogicalSynchronizerUpgradeCallback {

  /** Trigger the upgrade to the successor.
    *
    * Prerequisite:
    *   - Synchronizer time is passed upgrade time
    *   - Successor is registered
    */
  def registerCallback(successor: SynchronizerSuccessor)(implicit traceContext: TraceContext): Unit
}

object LogicalSynchronizerUpgradeCallback {
  val NoOp: LogicalSynchronizerUpgradeCallback = new LogicalSynchronizerUpgradeCallback {
    override def registerCallback(successor: SynchronizerSuccessor)(implicit
        traceContext: TraceContext
    ): Unit = ()
  }
}

class LogicalSynchronizerUpgradeCallbackImpl(
    psid: PhysicalSynchronizerId,
    synchronizerTimeTracker: SynchronizerTimeTracker,
    synchronizerConnectionsManager: SynchronizerConnectionsManager,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LogicalSynchronizerUpgradeCallback
    with NamedLogging {

  private val registered: AtomicBoolean = new AtomicBoolean(false)

  def registerCallback(
      successor: SynchronizerSuccessor
  )(implicit traceContext: TraceContext): Unit =
    if (registered.compareAndSet(false, true)) {
      logger.info(s"Registering callback for upgrade of $psid to $successor")

      synchronizerTimeTracker
        .awaitTick(successor.upgradeTime)
        .getOrElse(Future.unit)
        .foreach { _ =>
          synchronizerConnectionsManager.upgradeSynchronizerTo(psid, successor).discard
        }
    } else
      logger.info(
        s"Not registering callback for upgrade of $psid to $successor because it was already done"
      )
}
