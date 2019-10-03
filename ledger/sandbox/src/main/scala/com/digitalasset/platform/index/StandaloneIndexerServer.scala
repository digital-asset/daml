// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.actor.ActorSystem
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.index.config.{Config, StartupMode}

import scala.concurrent.duration._

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private[this] val actorSystem = ActorSystem("StandaloneIndexerServer")

  def apply(
      readService: ReadService,
      config: Config,
      loggerFactory: NamedLoggerFactory): AutoCloseable = {

    val indexerFactory = JdbcIndexerFactory(loggerFactory)
    val indexer =
      RecoveringIndexer(
        actorSystem.scheduler,
        10.seconds,
        indexerFactory.asyncTolerance,
        loggerFactory)

    config.startupMode match {
      case StartupMode.MigrateOnly =>
        indexerFactory.migrateSchema(config.jdbcUrl)

      case StartupMode.MigrateAndStart =>
        indexer.start { () =>
          indexerFactory
            .migrateSchema(config.jdbcUrl)
            .create(actorSystem, readService, config.jdbcUrl)
            .flatMap(_.subscribe(readService))(DEC)
        }

      case StartupMode.ValidateAndStart =>
        indexer.start { () =>
          indexerFactory
            .validateSchema(config.jdbcUrl)
            .create(actorSystem, readService, config.jdbcUrl)
            .flatMap(_.subscribe(readService))(DEC)
        }
    }

    indexer
  }
}
