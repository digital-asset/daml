// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  LedgerFactory,
  ParticipantConfig,
  ReadWriteService,
}
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import scopt.OptionParser

object SqlLedgerFactory extends LedgerFactory[ReadWriteService, ExtraConfig] {
  override val defaultExtraConfig: ExtraConfig = ExtraConfig(
    jdbcUrl = None
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
    config.copy(participants =
      config.participants.map(participantConfig =>
        participantConfig.copy(indexerConfig =
          participantConfig.indexerConfig.copy(allowExistingSchema = true)
        )
      )
    )

  override def readWriteServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[ReadWriteService] =
    new Owner(config, participantConfig, engine)

  class Owner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit loggingContext: LoggingContext)
      extends ResourceOwner[KeyValueParticipantState] {
    override def acquire()(implicit
        context: ResourceContext
    ): Resource[KeyValueParticipantState] = {
      val jdbcUrl = config.extra.jdbcUrl.getOrElse {
        throw new IllegalStateException("No JDBC URL provided.")
      }
      val metrics = createMetrics(participantConfig, config)
      new SqlLedgerReaderWriter.Owner(
        ledgerId = config.ledgerId,
        participantId = participantConfig.participantId,
        metrics = metrics,
        engine = engine,
        jdbcUrl = jdbcUrl,
        resetOnStartup = false,
        logEntryIdAllocator = RandomLogEntryIdAllocator,
        stateValueCache = caching.WeightedCache.from(
          configuration = config.stateValueCache,
          metrics = metrics.daml.kvutils.submission.validator.stateValueCache,
        ),
      ).acquire()
        .map(readerWriter =>
          new KeyValueParticipantState(
            readerWriter,
            readerWriter,
            metrics,
            enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
          )
        )
    }
  }
}
