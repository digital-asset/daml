// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  LedgerFactory,
  ParticipantConfig,
  ReadWriteService
}
import com.daml.ledger.participant.state.v1.SeedService
import com.daml.logging.LoggingContext
import com.daml.resources.{Resource, ResourceOwner}
import scopt.OptionParser

import scala.concurrent.ExecutionContext

object SqlLedgerFactory extends LedgerFactory[ReadWriteService, ExtraConfig] {
  override val defaultExtraConfig: ExtraConfig = ExtraConfig(
    jdbcUrl = None,
  )

  override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
    parser
      .opt[String]("jdbc-url")
      .required()
      .text("The URL used to connect to the database.")
      .action((jdbcUrl, config) => config.copy(extra = config.extra.copy(jdbcUrl = Some(jdbcUrl))))
    ()
  }

  override def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config.copy(participants = config.participants.map(_.copy(allowExistingSchemaForIndex = true)))

  override def readWriteServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig
  )(implicit materializer: Materializer, logCtx: LoggingContext): ResourceOwner[ReadWriteService] =
    new Owner(config, participantConfig)

  class Owner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig
  )(implicit materializer: Materializer, logCtx: LoggingContext)
      extends ResourceOwner[KeyValueParticipantState] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[KeyValueParticipantState] = {
      val jdbcUrl = config.extra.jdbcUrl.getOrElse {
        throw new IllegalStateException("No JDBC URL provided.")
      }
      val metrics = metricRegistry(participantConfig, config)
      new SqlLedgerReaderWriter.Owner(
        config.ledgerId,
        participantConfig.participantId,
        metrics,
        jdbcUrl,
        seedService = SeedService(config.seeding),
      ).acquire()
        .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter, metrics))
    }
  }

}
