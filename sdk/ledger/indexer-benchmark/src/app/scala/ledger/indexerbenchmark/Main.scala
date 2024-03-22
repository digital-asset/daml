// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.indexer.ha.EndlessReadService

import scala.concurrent.Future

object Main {

  def main(args: Array[String]): Unit =
    Config.parse(args) match {
      case Some(config) =>
        IndexerBenchmark.runAndExit(config, () => Future.successful(loadStateUpdates(config)))
      case None => sys.exit(1)
    }

  /* Note: this is a stub implementation. It doesn't provide meaningful performance numbers because:
   * - EndlessReadService is currently throttled, and the Akka throttle operator is a performance bottleneck
   * - EndlessReadService generates very simple transaction shapes that are not representative of
   *   the load on a real ledger.
   */
  private[this] def loadStateUpdates(config: Config): Source[(Offset, Update), NotUsed] =
    newLoggingContext { implicit loggingContext =>
      val readService = new EndlessReadService(updatesPerSecond = 10, name = config.updateSource)
      config.updateCount match {
        case None => readService.stateUpdates(None)
        case Some(count) => readService.stateUpdates(None).take(count)
      }
    }
}
