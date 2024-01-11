// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import scala.concurrent.{ExecutionContext, Future, Promise}

/** Allows instances to flag when they have actually closed.
  * Works around that FlagClosable only has one concurrent caller perform onClosed and other callers may not know when
  * this has completed.
  * TODO(#5705) This is overly complex and custom, revisit subscription pool closing
  */
trait CloseNotification {
  private val closedP = Promise[Unit]()

  val closedF: Future[Unit] = closedP.future

  def onClosed(callback: () => Unit)(implicit executionContext: ExecutionContext): Unit =
    closedP.future
      .onComplete { _ =>
        callback()
      }

  protected def notifyClosed(): Unit = closedP.success(())
}
