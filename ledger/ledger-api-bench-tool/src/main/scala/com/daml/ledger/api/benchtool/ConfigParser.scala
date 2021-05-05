// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import scopt.OParser

object ConfigParser {
  private val TransactionsStream = "Transactions"
  private val TransactionTreesStream = "TransactionTrees"

  def parse(args: Array[String]): Option[Config] =
    OParser.parse(parser, args, defaultConfig)

  private val parser = {
    val builder = OParser.builder[Config]
    import builder._
    OParser.sequence(
      programName("ledger-api-bench-tool"),
      head("A tool for measuring transaction streaming performance of a ledger."),
      opt[String]("streamType")
        .valueName("<streamType>")
        .maxOccurs(1)
        .text(
          s"Stream type for benchmarking. $TransactionsStream|$TransactionTreesStream (default: $TransactionsStream)"
        )
        .action { case (streamTypeInput, config) =>
          val streamType: Config.StreamType = streamTypeInput match {
            case TransactionsStream => Config.StreamType.Transactions
            case TransactionTreesStream => Config.StreamType.TransactionTrees
            case invalid => throw new IllegalArgumentException(s"Illegal stream type: $invalid")
          }
          config.copy(streamType = streamType)
        },
      help("help").text("Prints this information"),
    )
  }

  private val defaultConfig: Config =
    Config(
      streamType = Config.StreamType.Transactions,
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
    )
}
