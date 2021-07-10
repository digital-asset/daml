// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean

import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.util.{Failure, Success, Try}

class PollingChecker(
    periodMillis: Long,
    checkBlock: => Unit,
    killSwitch: KillSwitch,
)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)

  private val timer = new Timer(true)

  private val lostMainConnectionEmulation = new AtomicBoolean(false)

  timer.scheduleAtFixedRate(
    new TimerTask {
      override def run(): Unit = {
        Try(check())
        ()
      }
    },
    periodMillis,
    periodMillis,
  )

  // TODO uncomment this for main-connection-lost simulation
  //    timer.schedule(
  //      new TimerTask {
  //        override def run(): Unit = lostMainConnectionEmulation.set(true)
  //      },
  //      20000,
  //    )

  // This is a cruel approach for ensuring single threaded usage of the mainConnection.
  // In theory this could have been made much more efficient: not enqueueing for a check of it's own,
  // but collecting requests, and replying in batches.
  // Although experiments show approx 1s until a full connection pool is initialized at first
  // (the peek scenario) which should be enough, and which can leave this code very simple.
  def check(): Unit = synchronized {
    logger.debug(s"Checking...")
    Try(checkBlock) match {
      case Success(_) if !lostMainConnectionEmulation.get =>
        logger.debug(s"Check successful.")

      case Success(_) =>
        logger.info(
          s"Check failed due to lost-main-connection simulation. KillSwitch/abort called."
        )
        killSwitch.abort(
          new Exception(
            "Check failed due to lost-main-connection simulation. KillSwitch/abort called."
          )
        )
        throw new Exception("Check failed due to lost-main-connection simulation.")

      case Failure(ex) =>
        logger.info(s"Check failed (${ex.getMessage}). KillSwitch/abort called.")
        killSwitch.abort(new Exception("check failed, killSwitch aborted", ex))
        throw ex
    }
  }

  def close(): Unit = timer.cancel()
}
