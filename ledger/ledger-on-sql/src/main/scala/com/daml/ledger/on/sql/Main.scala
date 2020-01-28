// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.ResourceOwner
import scopt.OptionParser

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  Runner("SQL Ledger", SqlLedgerFactory).run(args)

  case class ExtraConfig(jdbcUrl: Option[String])

  object SqlLedgerFactory extends LedgerFactory[SqlLedgerReaderWriter, ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = ExtraConfig(
      jdbcUrl = None,
    )

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
      parser
        .opt[String]("jdbc-url")
        .required()
        .text("The URL used to connect to the database.")
        .action((jdbcUrl, config) =>
          config.copy(extra = config.extra.copy(jdbcUrl = Some(jdbcUrl))),
        )
      ()
    }

    override def owner(
        ledgerId: LedgerId,
        participantId: ParticipantId,
        config: ExtraConfig,
    )(implicit materializer: Materializer): ResourceOwner[SqlLedgerReaderWriter] = {
      val jdbcUrl = config.jdbcUrl.getOrElse {
        throw new IllegalStateException("No JDBC URL provided.")
      }
      newLoggingContext { implicit logCtx =>
        SqlLedgerReaderWriter.owner(
          ledgerId = ledgerId,
          participantId = participantId,
          jdbcUrl = jdbcUrl,
        )
      }
    }
  }
}
