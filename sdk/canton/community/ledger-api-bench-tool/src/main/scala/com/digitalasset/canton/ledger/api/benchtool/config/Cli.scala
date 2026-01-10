// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.config

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{PemFile, TlsClientCertificate, TlsClientConfig}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import scopt.{OptionParser, Read}

import java.io.File
import java.nio.file.Paths
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}

object Cli {
  private val ProgramName: String = "ledger-api-bench-tool"
  private val parser: OptionParser[Config] = new OptionParser[Config](ProgramName) {
    import Reads.*

    head("A tool for measuring transaction streaming performance of a ledger.").discard

    opt[(String, Int)]("endpoint")(endpointRead)
      .abbr("e")
      .text("Ledger API endpoint")
      .valueName("<hostname>:<port>")
      .optional()
      .action { case ((hostname, port), config) =>
        config.copy(ledger = config.ledger.copy(hostname = hostname, port = port))
      }
      .discard

    opt[String]("indexdb-jdbc-url")
      .text("JDBC url to an IndexDB instance")
      .optional()
      .action { case (url, config) => config.withLedgerConfig(_.copy(indexDbJdbcUrlO = Some(url))) }
      .discard

    opt[WorkflowConfig.StreamConfig]("consume-stream")
      .abbr("s")
      .optional()
      .unbounded()
      .text(
        s"Stream configuration."
      )
      .valueName(
        "<param1>=<value1>,<param2>=<value2>,..."
      )
      .action { case (streamConfig, config) =>
        config
          .copy(workflow = config.workflow.copy(streams = config.workflow.streams :+ streamConfig))
      }
      .discard

    opt[File]("workflow-config")
      .hidden() // TODO(#12064): uncomment when production-ready
      .abbr("w")
      .optional()
      .text(
        "A workflow configuration file. Parameters defined via this method take precedence over --consume-stream options."
      )
      .action { case (workflowConfigFile, config) =>
        config.copy(workflowConfigFile = Some(workflowConfigFile))
      }
      .discard

    opt[Int]("max-in-flight-commands")
      .hidden() // TODO(#12064): uncomment when production-ready
      .text("Maximum in-flight commands for command submissions.")
      .optional()
      .action { case (size, config) =>
        config.copy(maxInFlightCommands = size)
      }
      .discard

    opt[Unit]("latency-test")
      .text("Run a SubmitAndWait latency benchmark")
      .optional()
      .action { case (_, config) => config.copy(latencyTest = true) }
      .discard

    opt[Long]("max-latency-millis")
      .text(
        "The maximum average latency allowed for latency benchmarks (in millis). Only relevant with `latency-test` enabled."
      )
      .optional()
      .action { case (maxLatencyMillis, config) =>
        config.copy(maxLatencyObjectiveMillis = maxLatencyMillis)
      }
      .discard

    opt[Int]("submission-batch-size")
      .hidden() // TODO(#12064): uncomment when production-ready
      .text("Number of contracts created per command submission.")
      .optional()
      .action { case (size, config) =>
        config.copy(submissionBatchSize = size)
      }
      .discard

    opt[FiniteDuration]("log-interval")
      .abbr("r")
      .text("Stream metrics log interval.")
      .action { case (period, config) => config.copy(reportingPeriod = period) }
      .discard

    opt[Int]("core-pool-size")
      .text("Initial size of the worker thread pool.")
      .optional()
      .action { case (size, config) =>
        config.copy(concurrency = config.concurrency.copy(corePoolSize = size))
      }
      .discard

    opt[Int]("max-pool-size")
      .text("Maximum size of the worker thread pool.")
      .optional()
      .action { case (size, config) =>
        config.copy(concurrency = config.concurrency.copy(maxPoolSize = size))
      }
      .discard

    opt[String]("user-based-authorization-secret")
      .optional()
      .text(
        "Enables user based authorization. The value is used for signing authorization tokens with HMAC256."
      )
      .action((secret, config) => config.copy(authorizationTokenSecret = Some(secret)))
      .discard

    opt[Seq[String]]("client-cert")
      .optional()
      .text(
        "TLS: The crt file to be used as the cert chain and the pem file to be used as the private key."
      )
      .validate {
        case certs @ Seq(_crt, _pem) =>
          certs
            .map(path =>
              validatePath(path, s"The file $path specified via --client-cert does not exist")
            )
            .find(_.isLeft)
            .getOrElse(Either.unit)
        case _ => Left("expected only two files for --client-cert")
      }
      .valueName("<crt>,<pem>")
      .action {
        case (Seq(crt, pem), config) =>
          config.copy(
            tls = config.tls.map(tls =>
              tls.copy(clientCert =
                Some(
                  TlsClientCertificate(
                    certChainFile = PemFile(ExistingFile.tryCreate(Paths.get(crt).toFile)),
                    privateKeyFile = PemFile(ExistingFile.tryCreate(Paths.get(pem).toFile)),
                  )
                )
              )
            )
          )
        case (_, config) => config
      }
      .discard

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the trusted root CA.")
      .validate(validatePath(_, "The file specified via --cacrt does not exist"))
      .action { (path, config) =>
        val file = Some(PemFile(ExistingFile.tryCreate(path)))
        config.copy(
          tls = config.tls
            .map(_.copy(trustCollectionFile = file))
            .orElse(
              Some(
                TlsClientConfig(
                  trustCollectionFile = file,
                  clientCert = None,
                )
              )
            )
        )
      }
      .discard

    // allows you to enable tls without any special certs,
    // i.e., tls without client auth with the default root certs.
    // If any certificates are set tls is enabled implicitly and
    // this is redundant.
    opt[Unit]("tls")
      .optional()
      .text("TLS: Enable tls. This is redundant if --client-cert or --cacrt are set")
      .action((_, config) =>
        config.copy(tls =
          config.tls.orElse(
            Some(
              TlsClientConfig(
                trustCollectionFile = None,
                clientCert = None,
              )
            )
          )
        )
      )
      .discard

    checkConfig(c =>
      Either.cond(
        !(c.latencyTest && c.workflow.streams.nonEmpty),
        (),
        "Latency test cannot have configured streams",
      )
    ).discard

    private def validatePath(path: String, message: String): Either[String, Unit] = {
      val valid = Try(Paths.get(path).toFile.canRead).getOrElse(false)
      Either.cond(valid, (), message)
    }

    help("help").text("Prints this information").discard

    private def note(level: Int, param: String, desc: String = ""): Unit = {
      val paddedParam = s"${" " * level * 2}$param"
      val internalPadding = math.max(1, 50 - paddedParam.length)
      note(s"$paddedParam${" " * internalPadding}$desc").discard
    }

    note(0, "")
    note(0, "Stream configuration parameters:")
    note(1, "Transactions/transactions LedgerEffects:")
    note(2, "stream-type=<transactions|transactions-ledger-effects>", "(required)")
    note(2, "name=<stream-name>", "Stream name used to identify results (required)")
    note(
      2,
      "filters=party1@template1@template2+party2",
      "List of per-party filters separated by the plus symbol (required)",
    )
    note(2, "begin-offset=<offset>")
    note(2, "end-offset=<offset>")
    note(2, "max-delay=<seconds>", "Max record time delay objective")
    note(2, "min-consumption-speed=<speed>", "Min consumption speed objective")
    note(2, "min-item-rate=<rate>", "Min item rate per second")
    note(2, "max-item-rate=<rate>", "Max item rate per second")
    note(1, "Active contract sets:")
    note(2, "stream-type=active-contracts", "(required)")
    note(2, "name=<stream-name>", "Stream name used to identify results (required)")
    note(
      2,
      "filters=party1@template1@template2+party2",
      "List of per-party filters separated by the plus symbol (required)",
    )
    note(2, "min-item-rate=<rate>", "Min item rate per second")
    note(2, "max-item-rate=<rate>", "Max item rate per second")
    note(1, "Command completions:")
    note(2, "stream-type=completions", "(required)")
    note(2, "name=<stream-name>", "Stream name used to identify results (required)")
    note(2, "party=<party>", "(required)")
    note(2, "begin-offset=<offset>")
    note(2, "template-ids=<id1>|<id2>")
    note(2, "min-item-rate=<rate>", "Min item rate per second")
    note(2, "max-item-rate=<rate>", "Max item rate per second")
  }

  def config(args: Array[String]): Option[Config] =
    parser.parse(args, Config.Default)

  private object Reads {
    implicit val streamConfigRead: Read[WorkflowConfig.StreamConfig] =
      Read.mapRead[String, String].map { m =>
        def stringField(fieldName: String): Either[String, String] =
          m.get(fieldName) match {
            case Some(value) => Right(value)
            case None => Left(s"Missing field: '$fieldName'")
          }

        def optionalLongField(fieldName: String): Either[String, Option[Long]] =
          optionalField[Long](fieldName, _.toLong)

        def optionalDoubleField(fieldName: String): Either[String, Option[Double]] =
          optionalField[Double](fieldName, _.toDouble)

        def optionalScalaDurationField(fieldName: String): Either[String, Option[FiniteDuration]] =
          optionalField[String](fieldName, identity).flatMap {
            case Some(value) =>
              Duration(value) match {
                case infinite: Duration.Infinite =>
                  Left(s"Subscription delay duration must be finite, but got $infinite")
                case finiteDuration: FiniteDuration => Right(Some(finiteDuration))
              }
            case None => Right(None)
          }

        def optionalField[T](fieldName: String, f: String => T): Either[String, Option[T]] =
          Try(m.get(fieldName).map(f)) match {
            case Success(value) => Right(value)
            case Failure(_) => Left(s"Invalid value for field name: $fieldName")
          }

        def transactionObjectives(
            maxDelaySeconds: Option[Long],
            minConsumptionSpeed: Option[Double],
            minItemRate: Option[Double],
            maxItemRate: Option[Double],
        ): Option[WorkflowConfig.StreamConfig.TransactionObjectives] =
          (maxDelaySeconds, minConsumptionSpeed, minItemRate, maxItemRate) match {
            case (None, None, None, None) => None
            case _ =>
              Some(
                WorkflowConfig.StreamConfig.TransactionObjectives(
                  maxDelaySeconds = maxDelaySeconds,
                  minConsumptionSpeed = minConsumptionSpeed,
                  minItemRate = minItemRate,
                  maxItemRate = maxItemRate,
                  // NOTE: Unsupported on CLI
                  maxTotalStreamRuntimeDuration = None,
                )
              )
          }

        def transactionsConfig
            : Either[String, WorkflowConfig.StreamConfig.TransactionsStreamConfig] = for {
          name <- stringField("name")
          filters <- stringField("filters").flatMap(parseFilters)
          beginOffset <- optionalLongField("begin-offset")
          endOffset <- optionalLongField("end-offset")
          maxDelaySeconds <- optionalLongField("max-delay")
          minConsumptionSpeed <- optionalDoubleField("min-consumption-speed")
          minItemRate <- optionalDoubleField("min-item-rate")
          maxItemRate <- optionalDoubleField("max-item-rate")
          maxItemCount <- optionalLongField("max-item-count")
          timeoutO <- optionalScalaDurationField("timeout")
          subscriptionDelayO <- optionalScalaDurationField("subscription-delay")
        } yield WorkflowConfig.StreamConfig.TransactionsStreamConfig(
          name = name,
          filters = filters,
          beginOffsetExclusive = beginOffset.getOrElse(0L),
          endOffsetInclusive = endOffset,
          objectives =
            transactionObjectives(maxDelaySeconds, minConsumptionSpeed, minItemRate, maxItemRate),
          timeoutO = timeoutO,
          maxItemCount = maxItemCount,
          // NOTE: Unsupported on CLI
          partyNamePrefixFilters = List.empty,
          subscriptionDelay = subscriptionDelayO,
        )

        def transactionLedgerEffectsConfig
            : Either[String, WorkflowConfig.StreamConfig.TransactionLedgerEffectsStreamConfig] =
          for {
            name <- stringField("name")
            filters <- stringField("filters").flatMap(parseFilters)
            beginOffset <- optionalLongField("begin-offset")
            endOffset <- optionalLongField("end-offset")
            maxDelaySeconds <- optionalLongField("max-delay")
            minConsumptionSpeed <- optionalDoubleField("min-consumption-speed")
            minItemRate <- optionalDoubleField("min-item-rate")
            maxItemRate <- optionalDoubleField("max-item-rate")
            maxItemCount <- optionalLongField("max-item-count")
            timeoutO <- optionalScalaDurationField("timeout")
            subscriptionDelayO <- optionalScalaDurationField("subscription-delay")
          } yield WorkflowConfig.StreamConfig.TransactionLedgerEffectsStreamConfig(
            name = name,
            filters = filters,
            beginOffsetExclusive = beginOffset.getOrElse(0L),
            endOffsetInclusive = endOffset,
            objectives =
              transactionObjectives(maxDelaySeconds, minConsumptionSpeed, minItemRate, maxItemRate),
            timeoutO = timeoutO,
            maxItemCount = maxItemCount,
            // NOTE: Unsupported on CLI
            partyNamePrefixFilters = List.empty,
            subscriptionDelay = subscriptionDelayO,
          )

        def rateObjectives(
            minItemRate: Option[Double],
            maxItemRate: Option[Double],
        ): Option[WorkflowConfig.StreamConfig.AcsAndCompletionsObjectives] =
          (minItemRate, maxItemRate) match {
            case (None, None) => None
            case _ =>
              Some(
                WorkflowConfig.StreamConfig.AcsAndCompletionsObjectives(
                  minItemRate = minItemRate,
                  maxItemRate = maxItemRate,
                  // NOTE: Unsupported on CLI
                  maxTotalStreamRuntimeDuration = None,
                )
              )
          }

        def activeContractsConfig
            : Either[String, WorkflowConfig.StreamConfig.ActiveContractsStreamConfig] = for {
          name <- stringField("name")
          filters <- stringField("filters").flatMap(parseFilters)
          minItemRate <- optionalDoubleField("min-item-rate")
          maxItemRate <- optionalDoubleField("max-item-rate")
          maxItemCount <- optionalLongField("max-item-count")
          timeout <- optionalScalaDurationField("timeout")
          subscriptionDelayO <- optionalScalaDurationField("subscription-delay")
        } yield WorkflowConfig.StreamConfig.ActiveContractsStreamConfig(
          name = name,
          filters = filters,
          objectives = rateObjectives(minItemRate, maxItemRate),
          timeoutO = timeout,
          maxItemCount = maxItemCount,
          // NOTE: Unsupported on CLI
          partyNamePrefixFilters = List.empty,
          subscriptionDelay = subscriptionDelayO,
        )

        def completionsConfig: Either[String, WorkflowConfig.StreamConfig.CompletionsStreamConfig] =
          for {
            name <- stringField("name")
            parties <- stringField("parties").map(parseParties)
            userId <- stringField("user-id")
            beginOffset <- optionalLongField("begin-offset")
            minItemRate <- optionalDoubleField("min-item-rate")
            maxItemRate <- optionalDoubleField("max-item-rate")
            timeoutO <- optionalScalaDurationField("timeout")
            maxItemCount <- optionalLongField("max-item-count")
            subscriptionDelayO <- optionalScalaDurationField("subscription-delay")
          } yield WorkflowConfig.StreamConfig.CompletionsStreamConfig(
            name = name,
            parties = parties,
            userId = userId,
            beginOffsetExclusive = beginOffset,
            objectives = rateObjectives(minItemRate, maxItemRate),
            timeoutO = timeoutO,
            maxItemCount = maxItemCount,
            subscriptionDelay = subscriptionDelayO,
          )

        val config = stringField("stream-type").flatMap[String, WorkflowConfig.StreamConfig] {
          case "transactions" => transactionsConfig
          case "transactions-ledger-effects" => transactionLedgerEffectsConfig
          case "active-contracts" => activeContractsConfig
          case "completions" => completionsConfig
          case invalid => Left(s"Invalid stream type: $invalid")
        }

        config.fold(error => throw new IllegalArgumentException(error), identity)
      }

    // Parse strings like: "", "party1" or "party1+party2+party3"
    private def parseParties(raw: String): List[String] =
      raw.split('+').toList

    private def parseFilters(
        listOfIds: String
    ): Either[String, List[WorkflowConfig.StreamConfig.PartyFilter]] =
      listOfIds
        .split('+')
        .toList
        .map(parseFilter)
        .foldLeft[Either[String, List[WorkflowConfig.StreamConfig.PartyFilter]]](
          Right(List.empty)
        ) { case (acc, next) =>
          for {
            filters <- acc
            filter <- next
          } yield filters :+ filter
        }

    private def parseFilter(
        filterString: String
    ): Either[String, WorkflowConfig.StreamConfig.PartyFilter] =
      filterString
        .split('@')
        .toList match {
        case party :: templates =>
          Right(
            WorkflowConfig.StreamConfig.PartyFilter(party, templates, List.empty)
          ) // Interfaces are not supported via Cli
        case _ => Left("Filter cannot be empty")
      }

    def endpointRead: Read[(String, Int)] = new Read[(String, Int)] {
      val arity = 1
      val reads: String => (String, Int) =
        splitAddress(_) match {
          case (k, v) => Read.stringRead.reads(k) -> Read.intRead.reads(v)
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
