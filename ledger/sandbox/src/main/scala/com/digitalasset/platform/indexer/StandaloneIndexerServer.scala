// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.indexer.StandaloneIndexerServer._
import com.digitalasset.platform.resources.{Resource, ResourceOwner}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
class StandaloneIndexerServer(
    readService: ReadService,
    config: IndexerConfig,
    loggerFactory: NamedLoggerFactory,
    metrics: MetricRegistry,
) extends ResourceOwner[Unit] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] =
    for {
      // ActorSystem name not allowed to contain daml-lf LedgerString characters ".:#/ "
      actorSystem <- ResourceOwner
        .forActorSystem(() =>
          ActorSystem("StandaloneIndexerServer-" + config.participantId.filterNot(".:#/ ".toSet)))
        .acquire()
      indexerFactory = JdbcIndexerFactory(metrics, loggerFactory)
      indexer = new RecoveringIndexer(
        actorSystem.scheduler,
        asyncTolerance,
        indexerFactory.asyncTolerance,
        loggerFactory,
      )
      _ <- config.startupMode match {
        case IndexerStartupMode.MigrateOnly =>
          Resource.pure(Future.successful(()))
        case IndexerStartupMode.MigrateAndStart =>
          startIndexer(indexer, indexerFactory.migrateSchema(config.jdbcUrl), actorSystem)
        case IndexerStartupMode.ValidateAndStart =>
          startIndexer(indexer, indexerFactory.validateSchema(config.jdbcUrl), actorSystem)
      }
      _ = loggerFactory.getLogger(getClass).debug("Waiting for indexer to initialize the database")
    } yield ()

  def startIndexer(
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

object StandaloneIndexerServer {
  private val asyncTolerance: FiniteDuration = 10.seconds
}
