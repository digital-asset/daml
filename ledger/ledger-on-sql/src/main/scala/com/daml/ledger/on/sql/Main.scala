// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{Config, KeyValueLedger, LedgerFactory, Runner}
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId}
import com.digitalasset.logging.LoggingContext.newLoggingContext
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object Main extends App {
  Runner("SQL Ledger", SqlLedgerFactory).run(args)

  case class ExtraConfig(jdbcUrl: Option[String])

  object SqlLedgerFactory extends LedgerFactory[ExtraConfig] {
    override val defaultExtraConfig: ExtraConfig = ExtraConfig(
      jdbcUrl = None,
    )

    override def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
      parser
        .opt[String]("jdbc-url")
        .required()
        .text("The URL used to connect to the database.")
        .action(
          (jdbcUrl, config) => config.copy(extra = config.extra.copy(jdbcUrl = Some(jdbcUrl))))
      ()
    }

    override def apply(ledgerId: LedgerId, participantId: ParticipantId, config: ExtraConfig)(
        implicit materializer: Materializer,
    ): KeyValueLedger = {
      val jdbcUrl = config.jdbcUrl.getOrElse {
        throw new IllegalStateException("No JDBC URL provided.")
      }
      Await.result(
        newLoggingContext { implicit loggingContext =>
          SqlLedgerReaderWriter(
            ledgerId = ledgerId,
            participantId = participantId,
            jdbcUrl = jdbcUrl,
          )
        },
        10.seconds,
      )
    }
  }
}
