// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.indexer.ha.EndlessReadService

import scala.concurrent.{ExecutionContext, Future}

object Main {
  val DefaultUpdateCount = 500000L

  def main(args: Array[String]): Unit =
    Config.parse(args) match {
      case Some(config) => IndexerBenchmark.runAndExit(config, () => loadStateUpdates(config))
      case None => sys.exit(1)
    }

  private[this] def loadStateUpdates(config: Config): Future[Iterator[(Offset, Update)]] = {
    val system = ActorSystem("IndexerBenchmarkUpdateReader")
    implicit val materializer: Materializer = Materializer(system)
    newLoggingContext { implicit loggingContext =>
      val readService = new EndlessReadService(config.updateSource)

      readService
        .stateUpdates(None)
        .take(config.updateCount.getOrElse(DefaultUpdateCount))
        .zipWithIndex
        .map { case (data, index) =>
          if (index % 1000 == 0) println(s"Generated update $index")
          data
        }
        .runWith(Sink.seq[(Offset, Update)])
        .map(seq => seq.iterator)(ExecutionContext.parasitic)
        .andThen { case _ => system.terminate() }(ExecutionContext.parasitic)
    }
  }
}
