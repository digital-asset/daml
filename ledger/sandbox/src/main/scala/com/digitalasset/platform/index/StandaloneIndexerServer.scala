// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.actor.ActorSystem
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.ledger.api.domain
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.index.config.Config

import scala.concurrent.duration._

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private[this] val actorSystem = ActorSystem("StandaloneIndexerServer")

  def apply(readService: ReadService, config: Config): AutoCloseable = {

    val indexer =
      RecoveringIndexer(actorSystem.scheduler, 10.seconds, JdbcIndexer.asyncTolerance)

    indexer.start(
      () =>
        JdbcIndexer
          .create(
            domain.ParticipantId(config.participantId),
            actorSystem,
            readService,
            config.jdbcUrl)
          .flatMap(_.subscribe(readService))(DEC))

    indexer
  }
}
