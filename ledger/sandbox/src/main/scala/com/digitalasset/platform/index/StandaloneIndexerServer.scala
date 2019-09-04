// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.actor.ActorSystem
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}

import scala.concurrent.duration._

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private[this] val actorSystem = ActorSystem("StandaloneIndexerServer")

  def apply(readService: ReadService, jdbcUrl: String): AutoCloseable = {

    val indexer =
      RecoveringIndexer(actorSystem.scheduler, 10.seconds, JdbcIndexer.asyncTolerance)

    indexer.start(
      () =>
        JdbcIndexer
          .create(actorSystem, readService, jdbcUrl)
          .flatMap(_.subscribe(readService))(DEC))

    indexer
  }
}
