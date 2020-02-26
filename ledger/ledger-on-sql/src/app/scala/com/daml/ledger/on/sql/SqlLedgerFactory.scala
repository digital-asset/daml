// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, ParticipantConfig}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

import scala.concurrent.ExecutionContext

object SqlLedgerFactory extends LedgerFactory[SqlLedgerReaderWriter, ExtraConfig] {
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

  override def owner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): ResourceOwner[SqlLedgerReaderWriter] = {
    val jdbcUrl = config.extra.jdbcUrl.getOrElse {
      throw new IllegalStateException("No JDBC URL provided.")
    }
    SqlLedgerReaderWriter.owner(
      config.ledgerId,
      participantConfig.participantId,
      jdbcUrl,
      SqlLedgerReaderWriter.DefaultTimeProvider
    )
  }
}
