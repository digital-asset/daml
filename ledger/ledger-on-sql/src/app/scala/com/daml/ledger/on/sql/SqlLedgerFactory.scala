// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  ConfigProvider,
  KeyValueReadWriteFactory,
  LedgerFactory,
  ParticipantConfig,
  ReadWriteServiceFactory,
}
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import scopt.OptionParser

import scala.concurrent.ExecutionContext

object SqlLedgerFactory extends LedgerFactory[ExtraConfig] {

  override def readWriteServiceFactoryOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ReadWriteServiceFactory] = {
    val jdbcUrl = config.extra.jdbcUrl.getOrElse {
      throw new IllegalStateException("No JDBC URL provided.")
    }
    new SqlLedgerReaderWriter.Owner(
      ledgerId = config.ledgerId,
      participantId = participantConfig.participantId,
      metrics = metrics,
      engine = engine,
      jdbcUrl = jdbcUrl,
      resetOnStartup = false,
      offsetVersion = 0,
      logEntryIdAllocator = RandomLogEntryIdAllocator,
      stateValueCache = caching.WeightedCache.from(
        configuration = config.stateValueCache,
        metrics = metrics.daml.kvutils.submission.validator.stateValueCache,
      ),
    ).map(ledgerReaderWriter =>
      new KeyValueReadWriteFactory(
        config,
        metrics,
        ledgerReaderWriter,
        ledgerReaderWriter,
      )
    )
  }
}

object SqlConfigProvider extends ConfigProvider[ExtraConfig] {

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

}
