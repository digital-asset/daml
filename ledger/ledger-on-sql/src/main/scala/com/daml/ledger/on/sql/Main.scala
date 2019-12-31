// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import com.daml.ledger.participant.state.kvutils.app.{Config, Runner}
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object Main extends App {
  Runner(
    "SQL Ledger",
    ExtraConfig.augmentParser,
    ExtraConfig.default,
    (participantId, config: ExtraConfig) =>
      Await.result(
        SqlLedgerReaderWriter(participantId = participantId, jdbcUrl = config.jdbcUrl.get),
        10.seconds),
  ).run(args)

  case class ExtraConfig(jdbcUrl: Option[String])

  object ExtraConfig {
    val default: ExtraConfig = ExtraConfig(
      jdbcUrl = None,
    )

    def augmentParser(parser: OptionParser[Config[ExtraConfig]]): Unit = {
      parser
        .opt[String]("jdbc-url")
        .required()
        .text("The URL used to connect to the database.")
        .action(
          (jdbcUrl, config) => config.copy(extra = config.extra.copy(jdbcUrl = Some(jdbcUrl))))
      ()
    }
  }

}
