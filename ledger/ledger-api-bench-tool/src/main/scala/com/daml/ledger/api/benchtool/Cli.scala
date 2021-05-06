// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import scopt.{OParser, Read}

object Cli {
  def config(args: Array[String]): Option[Config] =
    OParser.parse(parser, args, defaultConfig)

  private val parser = {
    val builder = OParser.builder[Config]
    import builder._
    import Reads._
    OParser.sequence(
      programName("ledger-api-bench-tool"),
      head("A tool for measuring transaction streaming performance of a ledger."),
      opt[String]("hostname")
        .abbr("h")
        .text("Ledger API hostname")
        .optional()
        .action((host, config) => config.copy(ledger = config.ledger.copy(hostname = host))),
      opt[Int]("port")
        .abbr("p")
        .text("Ledger API port")
        .optional()
        .action((port, config) => config.copy(ledger = config.ledger.copy(port = port))),
      opt[Config.StreamConfig]("consume-stream")
        .abbr("s")
        .text(
          s"Stream configuration."
        )
        .valueName("streamType=<transactions|transactionTrees>,name=<streamName>,party=<party>")
        .action { case (streamConfig, config) => config.copy(streamConfig = Some(streamConfig)) },
      help("help").text("Prints this information"),
    )
  }

  private val defaultConfig: Config =
    Config(
      ledger = Config.Ledger(
        hostname = "localhost",
        port = 6865,
      ),
      concurrency = Config.Concurrency(
        corePoolSize = 2,
        maxPoolSize = 8,
        keepAliveTime = 30,
        maxQueueLength = 10000,
      ),
      streamConfig = None,
    )

  private object Reads {
    implicit val streamConfigRead: Read[Config.StreamConfig] =
      implicitly[Read[Map[String, String]]].map { m =>
        def stringField(fieldName: String): Either[String, String] =
          m.get(fieldName) match {
            case Some(value) => Right(value)
            case None => Left(s"Missing field: '$fieldName'")
          }

        val config = for {
          name <- stringField("name")
          party <- stringField("party")
          streamType <- stringField("streamType").flatMap[String, Config.StreamConfig.StreamType] {
            case "transactions" => Right(Config.StreamConfig.StreamType.Transactions)
            case "transactionTrees" => Right(Config.StreamConfig.StreamType.TransactionTrees)
            case invalid => Left(s"Invalid stream type: $invalid")
          }
        } yield Config.StreamConfig(
          name = name,
          streamType = streamType,
          party = party,
        )

        config.fold(error => throw new IllegalArgumentException(error), identity)
      }
  }

}
