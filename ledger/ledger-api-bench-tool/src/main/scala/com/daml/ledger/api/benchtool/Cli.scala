// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.Config.StreamConfig
import com.daml.ledger.api.tls.TlsConfigurationCli
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.Identifier
import com.daml.metrics.MetricsReporter
import scopt.{OptionDef, OptionParser, Read}

import java.io.File
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object Cli {
  private val ProgramName: String = "ledger-api-bench-tool"
  private val parser: OptionParser[Config] = new OptionParser[Config](ProgramName) {
    import Reads._

    head("A tool for measuring transaction streaming performance of a ledger.")

    opt[(String, Int)]("endpoint")(endpointRead)
      .abbr("e")
      .text("Ledger API endpoint")
      .valueName("<hostname>:<port>")
      .optional()
      .action { case ((hostname, port), config) =>
        config.copy(ledger = config.ledger.copy(hostname = hostname, port = port))
      }

    opt[Config.StreamConfig]("consume-stream")
      .abbr("s")
      .unbounded()
      .text(
        s"Stream configuration."
      )
      .valueName(
        "<param1>=<value1>,<param2>=<value2>,..."
      )
      .action { case (streamConfig, config) =>
        config.copy(streams = config.streams :+ streamConfig)
      }

    opt[File]("contract-set-descriptor")
      .hidden() // TODO: uncomment when production-ready
      .abbr("d")
      .optional()
      .text("A contract set descriptor file.")
      .action { case (descriptorFile, config) =>
        config.copy(contractSetDescriptorFile = Some(descriptorFile))
      }

    opt[Int]("max-in-flight-commands")
      .hidden() // TODO: uncomment when production-ready
      .text("Maximum in-flight commands for command submissions.")
      .optional()
      .action { case (size, config) =>
        config.copy(maxInFlightCommands = size)
      }

    opt[Int]("submission-batch-size")
      .hidden() // TODO: uncomment when production-ready
      .text("Number of contracts created per command submission.")
      .optional()
      .action { case (size, config) =>
        config.copy(submissionBatchSize = size)
      }

    opt[FiniteDuration]("log-interval")
      .abbr("r")
      .text("Stream metrics log interval.")
      .action { case (period, config) => config.copy(reportingPeriod = period) }

    opt[Int]("core-pool-size")
      .text("Initial size of the worker thread pool.")
      .optional()
      .action { case (size, config) =>
        config.copy(concurrency = config.concurrency.copy(corePoolSize = size))
      }

    opt[Int]("max-pool-size")
      .text("Maximum size of the worker thread pool.")
      .optional()
      .action { case (size, config) =>
        config.copy(concurrency = config.concurrency.copy(maxPoolSize = size))
      }

    opt[MetricsReporter]("metrics-reporter")
      .optional()
      .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")
      .action((reporter, config) => config.copy(metricsReporter = reporter))

    TlsConfigurationCli.parse(parser = this, colSpacer = "        ")((f, c) =>
      c.copy(tls = f(c.tls))
    )

    help("help").text("Prints this information")

    private def note(level: Int, param: String, desc: String = ""): OptionDef[Unit, Config] = {
      val paddedParam = s"${" " * level * 2}$param"
      val internalPadding = math.max(1, 40 - paddedParam.length)
      note(s"$paddedParam${" " * internalPadding}$desc")
    }

    note(0, "")
    note(0, "Stream configuration parameters:")
    note(1, "Transactions/transaction trees:")
    note(2, "stream-type=<transactions|transaction-trees>", "(required)")
    note(2, "name=<stream-name>", "Stream name used to identify results (required)")
    note(2, "party=<party>", "(required)")
    note(2, "begin-offset=<offset>")
    note(2, "end-offset=<offset>")
    note(2, "template-ids=<id1>|<id2>")
    note(2, "max-delay=<seconds>", "Max record time delay objective")
    note(2, "min-consumption-speed=<speed>", "Min consumption speed objective")
    note(1, "Active contract sets:")
    note(2, "stream-type=active-contracts", "(required)")
    note(2, "name=<stream-name>", "Stream name used to identify results (required)")
    note(2, "party=<party>", "(required)")
    note(2, "template-ids=<id1>|<id2>")
    note(1, "Command completions:")
    note(2, "stream-type=completions", "(required)")
    note(2, "name=<stream-name>", "Stream name used to identify results (required)")
    note(2, "party=<party>", "(required)")
    note(2, "begin-offset=<offset>")
    note(2, "template-ids=<id1>|<id2>")
  }

  def config(args: Array[String]): Option[Config] =
    parser.parse(args, Config.Default)

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

        def transactionsConfig: Either[String, StreamConfig.TransactionsStreamConfig] = for {
          name <- stringField("name")
          party <- stringField("party")
          templateIds <- optionalStringField("template-ids").flatMap {
            case Some(ids) => listOfTemplateIds(ids).map(Some(_))
            case None => Right(None)
          }
          beginOffset <- optionalStringField("begin-offset").map(_.map(offset))
          endOffset <- optionalStringField("end-offset").map(_.map(offset))
          maxDelaySeconds <- optionalLongField("max-delay")
          minConsumptionSpeed <- optionalDoubleField("min-consumption-speed")
        } yield Config.StreamConfig.TransactionsStreamConfig(
          name = name,
          party = party,
          templateIds = templateIds,
          beginOffset = beginOffset,
          endOffset = endOffset,
          objectives = Config.StreamConfig.Objectives(
            maxDelaySeconds = maxDelaySeconds,
            minConsumptionSpeed = minConsumptionSpeed,
          ),
        )

        def transactionTreesConfig: Either[String, StreamConfig.TransactionTreesStreamConfig] =
          for {
            name <- stringField("name")
            party <- stringField("party")
            templateIds <- optionalStringField("template-ids").flatMap {
              case Some(ids) => listOfTemplateIds(ids).map(Some(_))
              case None => Right(None)
            }
            beginOffset <- optionalStringField("begin-offset").map(_.map(offset))
            endOffset <- optionalStringField("end-offset").map(_.map(offset))
            maxDelaySeconds <- optionalLongField("max-delay")
            minConsumptionSpeed <- optionalDoubleField("min-consumption-speed")
          } yield Config.StreamConfig.TransactionTreesStreamConfig(
            name = name,
            party = party,
            templateIds = templateIds,
            beginOffset = beginOffset,
            endOffset = endOffset,
            objectives = Config.StreamConfig.Objectives(
              maxDelaySeconds = maxDelaySeconds,
              minConsumptionSpeed = minConsumptionSpeed,
            ),
          )

        def activeContractsConfig: Either[String, StreamConfig.ActiveContractsStreamConfig] = for {
          name <- stringField("name")
          party <- stringField("party")
          templateIds <- optionalStringField("template-ids").flatMap {
            case Some(ids) => listOfTemplateIds(ids).map(Some(_))
            case None => Right(None)
          }
        } yield Config.StreamConfig.ActiveContractsStreamConfig(
          name = name,
          party = party,
          templateIds = templateIds,
        )

        def completionsConfig: Either[String, StreamConfig.CompletionsStreamConfig] = for {
          name <- stringField("name")
          party <- stringField("party")
          applicationId <- stringField("application-id")
          beginOffset <- optionalStringField("begin-offset").map(_.map(offset))
        } yield Config.StreamConfig.CompletionsStreamConfig(
          name = name,
          party = party,
          applicationId = applicationId,
          beginOffset = beginOffset,
        )

        val config = stringField("stream-type").flatMap[String, Config.StreamConfig] {
          case "transactions" => transactionsConfig
          case "transaction-trees" => transactionTreesConfig
          case "active-contracts" => activeContractsConfig
          case "completions" => completionsConfig
          case invalid => Left(s"Invalid stream type: $invalid")
        }

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
