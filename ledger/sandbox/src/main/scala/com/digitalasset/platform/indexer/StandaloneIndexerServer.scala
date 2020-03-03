// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
final class StandaloneIndexerServer(
    readService: ReadService,
    config: IndexerConfig,
    metrics: MetricRegistry,
)(implicit logCtx: LoggingContext)
    extends ResourceOwner[Unit] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] =
    for {
      // ActorSystem name not allowed to contain daml-lf LedgerString characters ".:#/ "
      actorSystem <- AkkaResourceOwner
        .forActorSystem(() =>
          ActorSystem("StandaloneIndexerServer-" + config.participantId.filterNot(".:#/ ".toSet)))
        .acquire()
      indexerFactory = JdbcIndexerFactory(metrics)
      indexer = new RecoveringIndexer(
        actorSystem.scheduler,
        config.restartDelay,
        indexerFactory.asyncTolerance
      )
      _ <- config.startupMode match {
        case IndexerStartupMode.MigrateOnly =>
          Resource.successful(Future.unit)
        case IndexerStartupMode.MigrateAndStart =>
          Resource
            .fromFuture(indexerFactory.migrateSchema(config.jdbcUrl, config.allowExistingSchema))
            .flatMap(startIndexer(indexer, _, actorSystem))
        case IndexerStartupMode.ValidateAndStart =>
          Resource
            .fromFuture(indexerFactory.validateSchema(config.jdbcUrl))
            .flatMap(startIndexer(indexer, _, actorSystem))
      }
    } yield {
      logger.debug("Waiting for indexer to initialize the database")
    }

  private def startIndexer(
      indexer: RecoveringIndexer,
      initializedIndexerFactory: JdbcIndexerFactory[Initialized],
      actorSystem: ActorSystem,
  )(implicit executionContext: ExecutionContext): Resource[Future[Unit]] =
    indexer
      .start(
        () =>
          initializedIndexerFactory
            .owner(config.participantId, actorSystem, readService, config.jdbcUrl)
            .flatMap(_.subscription(readService))
            .acquire())
}
