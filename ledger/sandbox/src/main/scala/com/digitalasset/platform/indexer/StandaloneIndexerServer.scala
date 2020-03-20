// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.configuration.ServerName
import com.digitalasset.platform.indexer.StandaloneIndexerServer._
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.ExecutionContext

final class StandaloneIndexerServer(
    actorSystem: ActorSystem,
    readService: ReadService,
    config: IndexerConfig,
    metrics: MetricRegistry,
)(implicit logCtx: LoggingContext)
    extends ResourceOwner[Unit] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] = {
    val indexerFactory = new JdbcIndexerFactory(
      Name,
      config.participantId,
      config.jdbcUrl,
      actorSystem,
      readService,
      metrics,
    )
    val indexer = new RecoveringIndexer(actorSystem.scheduler, config.restartDelay)
    config.startupMode match {
      case IndexerStartupMode.MigrateOnly =>
        Resource.successful(())
      case IndexerStartupMode.MigrateAndStart =>
        Resource
          .fromFuture(indexerFactory.migrateSchema(config.allowExistingSchema))
          .flatMap(startIndexer(indexer, _, actorSystem))
          .map { _ =>
            logger.debug("Waiting for the indexer to initialize the database.")
          }
      case IndexerStartupMode.ValidateAndStart =>
        Resource
          .fromFuture(indexerFactory.validateSchema())
          .flatMap(startIndexer(indexer, _, actorSystem))
          .map { _ =>
            logger.debug("Waiting for the indexer to initialize the database.")
          }
    }
  }

  private def startIndexer(
      indexer: RecoveringIndexer,
      initializedIndexerFactory: ResourceOwner[JdbcIndexer],
      actorSystem: ActorSystem,
  )(implicit executionContext: ExecutionContext): Resource[Unit] =
    indexer
      .start(() => initializedIndexerFactory.flatMap(_.subscription(readService)).acquire())
      .map(_ => ())
}

object StandaloneIndexerServer {
  private val Name: ServerName = ServerName("indexer")
}
