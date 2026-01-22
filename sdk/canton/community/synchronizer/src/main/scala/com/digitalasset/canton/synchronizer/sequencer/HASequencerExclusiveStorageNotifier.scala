// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.HASequencerExclusiveStorageNotifier.{
  FailoverNotification,
  FailoverNotifications,
  NoOpNotifications,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell

/** The HA-sequencer exclusive storage notifier allows components dependent on exclusive storage to
  * register for notifications when exclusive-storage failover occurs, so that exclusive storage
  * writes can be moved to the active instance.
  *
  * This allows building exclusive storage before components depending on exclusive storage and to
  * register for notifications in a delayed fashion after those components have been created.
  */
class HASequencerExclusiveStorageNotifier(protected val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {
  private val notifications: SingleUseCell[FailoverNotifications] =
    new SingleUseCell[FailoverNotifications]

  def setNotifications(onActive: FailoverNotification, onPassive: FailoverNotification): Unit =
    this.notifications.putIfAbsent(FailoverNotifications(onActive, onPassive)).foreach { _ =>
      logger.warn(s"Failed to set notifications, was already initialized")(
        TraceContext.empty
      )
    }

  def onActive(): FutureUnlessShutdown[Unit] =
    getNotificationOrElseLogWarning(onActive = true)
  def onPassive(): FutureUnlessShutdown[Unit] =
    getNotificationOrElseLogWarning(onActive = false)

  private def getNotificationOrElseLogWarning(onActive: Boolean): FutureUnlessShutdown[Unit] = {
    val notifications = this.notifications.getOrElse {
      logger.warn(
        s"Observed ${if (onActive) "onActive" else "onPassive"} notification before notifier initialized"
      )(TraceContext.empty)
      NoOpNotifications
    }
    if (onActive) notifications.onActive() else notifications.onPassive()
  }
}

object HASequencerExclusiveStorageNotifier {
  type FailoverNotification = () => FutureUnlessShutdown[Unit]
  sealed case class FailoverNotifications(
      onActive: FailoverNotification,
      onPassive: FailoverNotification,
  )

  object NoOpNotifications
      extends FailoverNotifications(
        onActive = () => FutureUnlessShutdown.unit,
        onPassive = () => FutureUnlessShutdown.unit,
      )
}
