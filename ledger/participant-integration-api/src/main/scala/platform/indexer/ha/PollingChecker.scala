// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.util.{Timer, TimerTask}

import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.util.{Failure, Success, Try}

/** A simple host of checking.
  * - This will ensure that check is accessed by only one caller at a time
  * - Does periodic checking
  * - Exposes check() for on-demand checking from the outside
  * - If whatever check() fails, it uses killSwitch with an abort
  * - It is also an AutoCloseable to release internal resources
  *
  * @param periodMillis period of the checking, between each scheduled checks there will be so much delay
  * @param checkBody the check function, Exception signals failed check
  * @param killSwitch to abort if a check fails
  */
class PollingChecker(
    periodMillis: Long,
    checkBody: => Unit,
    killSwitch: KillSwitch,
)(implicit loggingContext: LoggingContext)
    extends AutoCloseable {
  private val logger = ContextualizedLogger.get(this.getClass)

  private val timer = new Timer(true)

  timer.schedule(
    new TimerTask {
      override def run(): Unit = {
        Try(check())
        ()
      }
    },
    periodMillis,
    periodMillis,
  )

  // This is a cruel approach for ensuring single threaded usage of the mainConnection.
  // In theory this could have been made much more efficient: not enqueueing for a check of it's own,
  // but collecting requests, and replying in batches.
  // Although experiments show approx 1s until a full connection pool is initialized at first
  // (the peak scenario) which should be enough, and which can leave this code very simple.
  def check(): Unit = synchronized {
    logger.debug(s"Checking...")
    Try(checkBody) match {
      case Success(_) =>
        logger.debug(s"Check successful.")

      case Failure(ex) =>
        logger.info(s"Check failed (${ex.getMessage}). KillSwitch/abort called.")
        killSwitch.abort(new Exception("check failed, killSwitch aborted", ex))
        throw ex
    }
  }

  def close(): Unit = timer.cancel()
}
