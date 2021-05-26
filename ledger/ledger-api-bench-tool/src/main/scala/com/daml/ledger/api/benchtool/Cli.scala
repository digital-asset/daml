// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.Identifier
import scopt.{OParser, Read}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object Cli {
  def config(args: Array[String]): Option[Config] =
    OParser.parse(parser, args, Config.Default)

  private val parser = {
    val builder = OParser.builder[Config]
    import Reads._
    import builder._
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
        .unbounded()
        .minOccurs(1)
        .text(
          s"Stream configuration."
        )
        .valueName(
          "stream-type=<transactions|transaction-trees>,name=<streamName>,party=<party>[,begin-offset=<offset>][,end-offset=<offset>][,template-ids=<id1>|<id2>][,max-delay=<seconds>]"
        )
        .action { case (streamConfig, config) =>
          config.copy(streams = config.streams :+ streamConfig)
        },
      opt[FiniteDuration]("log-interval")
        .abbr("r")
        .text("Stream metrics log interval.")
        .action { case (period, config) => config.copy(reportingPeriod = period) },
      opt[Int]("core-pool-size")
        .text("Initial size of the worker thread pool.")
        .optional()
        .action { case (size, config) =>
          config.copy(concurrency = config.concurrency.copy(corePoolSize = size))
        },
      opt[Int]("max-pool-size")
        .text("Maximum size of the worker thread pool.")
        .optional()
        .action { case (size, config) =>
          config.copy(concurrency = config.concurrency.copy(maxPoolSize = size))
        },
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

        def optionalStringField(fieldName: String): Either[String, Option[String]] =
          Right(m.get(fieldName))

        def optionalLongField(fieldName: String): Either[String, Option[Long]] =
          optionalField[Long](fieldName, _.toLong)

        def optionalDoubleField(fieldName: String): Either[String, Option[Double]] =
          optionalField[Double](fieldName, _.toDouble)

        def optionalField[T](fieldName: String, f: String => T): Either[String, Option[T]] = {
          Try(m.get(fieldName).map(f)) match {
            case Success(value) => Right(value)
            case Failure(_) => Left(s"Invalid value for field name: $fieldName")
          }
        }

        def offset(stringValue: String): LedgerOffset =
          LedgerOffset.defaultInstance.withAbsolute(stringValue)

        val config = for {
          name <- stringField("name")
          party <- stringField("party")
          streamType <- stringField("stream-type").flatMap[String, Config.StreamConfig.StreamType] {
            case "transactions" => Right(Config.StreamConfig.StreamType.Transactions)
            case "transaction-trees" => Right(Config.StreamConfig.StreamType.TransactionTrees)
            case invalid => Left(s"Invalid stream type: $invalid")
          }
          templateIds <- optionalStringField("template-ids").flatMap {
            case Some(ids) => listOfTemplateIds(ids).map(Some(_))
            case None => Right(None)
          }
          beginOffset <- optionalStringField("begin-offset").map(_.map(offset))
          endOffset <- optionalStringField("end-offset").map(_.map(offset))
          maxDelaySeconds <- optionalLongField("max-delay")
          minConsumptionSpeed <- optionalDoubleField("min-consumption-speed")
        } yield Config.StreamConfig(
          name = name,
          streamType = streamType,
          party = party,
          templateIds = templateIds,
          beginOffset = beginOffset,
          endOffset = endOffset,
          objectives = Config.StreamConfig.Objectives(
            maxDelaySeconds = maxDelaySeconds,
            minConsumptionSpeed = minConsumptionSpeed,
          ),
        )

        config.fold(error => throw new IllegalArgumentException(error), identity)
      }

    private def listOfTemplateIds(listOfIds: String): Either[String, List[Identifier]] =
      listOfIds
        .split('|')
        .toList
        .map(templateIdFromString)
        .foldLeft[Either[String, List[Identifier]]](Right(List.empty[Identifier])) {
          case (acc, next) =>
            for {
              ids <- acc
              id <- next
            } yield id :: ids
        }

    private def templateIdFromString(fullyQualifiedTemplateId: String): Either[String, Identifier] =
      fullyQualifiedTemplateId
        .split(':')
        .toList match {
        case packageId :: moduleName :: entityName :: Nil =>
          Right(
            Identifier.defaultInstance
              .withEntityName(entityName)
              .withModuleName(moduleName)
              .withPackageId(packageId)
          )
        case _ =>
          Left(s"Invalid template id: $fullyQualifiedTemplateId")
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
