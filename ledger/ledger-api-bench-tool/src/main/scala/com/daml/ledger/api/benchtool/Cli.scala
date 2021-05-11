// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import scopt.{OParser, Read}

import scala.concurrent.duration.Duration

object Cli {
  def config(args: Array[String]): Option[Config] =
    OParser.parse(parser, args, Config.Default)

  private val parser = {
    val builder = OParser.builder[Config]
    import builder._
    import Reads._
    OParser.sequence(
      programName("ledger-api-bench-tool"),
      head("A tool for measuring transaction streaming performance of a ledger."),
      opt[(String, Int)]("endpoint")(endpointRead)
        .abbr("e")
        .text("Ledger API endpoint")
        .valueName("<hostname>:<port>")
        .optional()
        .action { case ((hostname, port), config) =>
          config.copy(ledger = config.ledger.copy(hostname = hostname, port = port))
        },
      opt[Config.StreamConfig]("consume-stream")
        .abbr("s")
        .text(
          s"Stream configuration."
        )
        .valueName("streamType=<transactions|transaction-trees>,name=<streamName>,party=<party>")
        .action { case (streamConfig, config) => config.copy(streamConfig = Some(streamConfig)) },
      opt[Duration]("log-interval")
        .abbr("r")
        .text("Stream metrics log interval.")
        .action { case (period, config) => config.copy(reportingPeriod = period) },
      help("help").text("Prints this information"),
    )
  }

  private object Reads {
    implicit val streamConfigRead: Read[Config.StreamConfig] =
      Read.mapRead[String, String].map { m =>
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
            case "transaction-trees" => Right(Config.StreamConfig.StreamType.TransactionTrees)
            case invalid => Left(s"Invalid stream type: $invalid")
          }
        } yield Config.StreamConfig(
          name = name,
          streamType = streamType,
          party = party,
        )

        config.fold(error => throw new IllegalArgumentException(error), identity)
      }

    def endpointRead: Read[(String, Int)] = new Read[(String, Int)] {
      val arity = 1
      val reads: String => (String, Int) = { s: String =>
        splitAddress(s) match {
          case (k, v) => Read.stringRead.reads(k) -> Read.intRead.reads(v)
        }
      }
    }

    private def splitAddress(s: String): (String, String) =
      s.indexOf(':') match {
        case -1 =>
          throw new IllegalArgumentException("Addresses should be specified as `<host>:<port>`")
        case n: Int => (s.slice(0, n), s.slice(n + 1, s.length))
      }
  }

}
