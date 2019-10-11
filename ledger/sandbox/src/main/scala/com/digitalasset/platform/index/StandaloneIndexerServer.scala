// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.actor.ActorSystem
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.index.config.{Config, StartupMode}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {

  def apply(
      readService: ReadService,
      config: Config,
      loggerFactory: NamedLoggerFactory): Future[AutoCloseable] = {

    val actorSystem = ActorSystem(config.participantId)
    val asyncTolerance: FiniteDuration = 10.seconds
    val indexerFactory = JdbcIndexerFactory(loggerFactory)
    val indexer =
      RecoveringIndexer(
        actorSystem.scheduler,
        asyncTolerance,
        indexerFactory.asyncTolerance,
        loggerFactory)

    val initializedIndexerFactory = config.startupMode match {
      case StartupMode.MigrateOnly | StartupMode.MigrateAndStart =>
        indexerFactory.migrateSchema(config.jdbcUrl)
      case StartupMode.ValidateAndStart => indexerFactory.validateSchema(config.jdbcUrl)
    }

    val promise = Promise[Unit]

    config.startupMode match {
      case StartupMode.MigrateOnly => promise.success(())
      case StartupMode.MigrateAndStart | StartupMode.ValidateAndStart =>
        indexer.start { () =>
          {
            val createF = initializedIndexerFactory
              .create(actorSystem, readService, config.jdbcUrl)
            // signal when ready
            createF.map(_ => {
              promise.trySuccess(())
            })(DEC)
            createF
              .flatMap(_.subscribe(readService))(DEC)
          }
        }
    }

    promise.future.map { _ =>
      loggerFactory.getLogger(getClass).debug("Waiting for indexer to initialize the database")
      new AutoCloseable {
        override def close(): Unit = {
          indexer.close()
          val _ = Await.result(actorSystem.terminate(), asyncTolerance)
        }
      }
    }(DEC)
  }
}
