// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.caching
import com.daml.ledger.participant.state.kvutils.api.KeyValueLedger
import com.daml.ledger.participant.state.kvutils.app.LedgerFactory.KeyValueLedgerFactory
import com.daml.ledger.participant.state.kvutils.app.{Config, ParticipantConfig}
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import scopt.OptionParser

object SqlLedgerFactory extends KeyValueLedgerFactory[KeyValueLedger, ExtraConfig] {
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

  override def owner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[KeyValueLedger] = {
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
      offsetVersion = 0,
      logEntryIdAllocator = RandomLogEntryIdAllocator,
      stateValueCache = caching.WeightedCache.from(
        configuration = config.stateValueCache,
        metrics = metrics.daml.kvutils.submission.validator.stateValueCache,
      ),
    )
  }

}
