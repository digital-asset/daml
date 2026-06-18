// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.data.SynchronizerSuccessor
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureUnlessShutdownUtil

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

trait LogicalSynchronizerUpgradeCallback {

  /** Trigger the upgrade to the successor.
    *
    * Prerequisite:
    *   - Synchronizer time is passed upgrade time
    *   - Successor is registered
    */
  def registerCallback(successor: SynchronizerSuccessor)(implicit traceContext: TraceContext): Unit

  def unregisterCallback(): Unit
}

object LogicalSynchronizerUpgradeCallback {
  val NoOp: LogicalSynchronizerUpgradeCallback = new LogicalSynchronizerUpgradeCallback {
    override def registerCallback(successor: SynchronizerSuccessor)(implicit
        traceContext: TraceContext
    ): Unit = ()

    override def unregisterCallback(): Unit = ()
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

  private val registered: AtomicReference[Option[SynchronizerSuccessor]] = new AtomicReference(None)

  def registerCallback(
      successor: SynchronizerSuccessor
  )(implicit traceContext: TraceContext): Unit =
    if (registered.compareAndSet(None, Some(successor))) {
      logger.info(s"Registering callback for upgrade of $psid to ${successor.psid}")

      synchronizerTimeTracker
        .awaitTick(successor.upgradeTime)
        .getOrElse(Future.unit)
        .foreach { _ =>
          if (registered.get().contains(successor)) {
            val upgradeResultF = synchronizerConnectionsManager
              .upgradeSynchronizerTo(psid, successor)
              .value
              .map(
                _.fold(err => logger.error(s"Upgrade to ${successor.psid} failed: $err"), _ => ())
              )

            FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
              upgradeResultF,
              s"Failed to upgrade to ${successor.psid}",
            )
          } else
            logger.info(s"Upgrade to ${successor.psid} was cancelled, not executing the upgrade.")
        }
    } else
      logger.info(
        s"Not registering callback for upgrade of $psid to ${successor.psid} because it was already done"
      )

  override def unregisterCallback(): Unit = registered.set(None)
}
