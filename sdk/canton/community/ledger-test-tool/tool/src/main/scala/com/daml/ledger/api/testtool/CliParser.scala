// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.testtool.runner.Config
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.config.{PemFile, TlsClientCertificate}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import scopt.{OptionParser, Read}

import java.io.File
import java.nio.file.Paths
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try
import scala.util.matching.Regex

object CliParser {
  private val Name = "ledger-api-test-tool"

  private implicit val fileRead: Read[File] = Read.reads(Paths.get(_).toFile)

  def parse(args: Array[String]): Option[Config] =
    argParser.parse(args, Config.default)

  private def endpointRead: Read[(String, Int)] = new Read[(String, Int)] {
    override val arity = 2
    override val reads: String => (String, Int) = { (s: String) =>
      splitAddress(s) match {
        case (k, v) => Read.stringRead.reads(k) -> Read.intRead.reads(v)
      }
    }
  }

  private def lapiAndOptionalAdminEndpointRead: Read[((String, Int), (String, Int))] =
    new Read[((String, Int), (String, Int))] {
      override val arity = 2
      override val reads: String => ((String, Int), (String, Int)) = { (s: String) =>
        s.split(';') match {
          case Array(lapiEndpoint, adminEndpoint) =>
            (endpointRead.reads(lapiEndpoint), endpointRead.reads(adminEndpoint))
          case _ =>
            throw new IllegalArgumentException(
              "both LAPI and Admin API addresses should be specified as `<lapiAddress>;<adminApiAddress>`"
            )
        }
      }
    }

  private def splitAddress(s: String): (String, String) =
    s.indexOf(':') match {
      case -1 =>
        throw new IllegalArgumentException("Addresses should be specified as `<host>:<port>`")
      case n: Int => (s.slice(0, n), s.slice(n + 1, s.length))
    }

  private val argParser: OptionParser[Config] = new scopt.OptionParser[Config](Name) {
    head(
      """The Ledger API Test Tool is a command line tool for testing the correctness of
        |ledger implementations based on Daml and Ledger API.""".stripMargin
    ).discard

    arg[((String, Int), (String, Int))]("[endpoints...]")(lapiAndOptionalAdminEndpointRead)
      .action { case ((lapiAddress, adminApiAddress), config) =>
        config.copy(
          participantsEndpoints = config.participantsEndpoints :+ lapiAddress,
          participantsAdminEndpoints = config.participantsAdminEndpoints :+ adminApiAddress,
        )
      }
      .unbounded()
      .optional()
      .text("Addresses of the participants to test, specified as `<host>:<port>`.")
      .discard

    opt[Int]("max-connection-attempts")
      .action((maxConnectionAttempts, config) =>
        config.copy(maxConnectionAttempts = maxConnectionAttempts)
      )
      .optional()
      .text("Number of connection attempts to the participants. Applied to all endpoints.")
      .discard

    opt[Unit]("json-api-mode")
      .action((_, config) => config.copy(jsonApiMode = true))
      .optional()
      .discard

    opt[Seq[File]]("client-cert")
      .optional()
      .text(
        "TLS: The crt file to be used as the cert chain and the pem file to be used as the private key."
      )
      .valueName("<crt>,<pem>")
      .action {
        case (Seq(crt, pem), config) =>
          config.withTlsConfig(cfg =>
            cfg.copy(clientCert =
              Some(
                TlsClientCertificate(
                  certChainFile = PemFile(ExistingFile.tryCreate(crt)),
                  privateKeyFile = PemFile(ExistingFile.tryCreate(pem)),
                )
              )
            )
          )
        case (_, config) => config
      }
      .discard

    opt[File]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the trusted root CA. Applied to all endpoints.")
      .action { (path: File, config: Config) =>
        config.withTlsConfig(
          _.copy(trustCollectionFile = Some(PemFile(ExistingFile.tryCreate(path))))
        )
      }
      .discard

    opt[Double](name = "timeout-scale-factor")
      .optional()
      .action((v, c) => c.copy(timeoutScaleFactor = v))
      .text(
        """Scale factor for timeouts used in all test suites. Useful to tune timeouts
          |depending on the environment and the Ledger implementation under test.
          |Defaults to 1.0. Use numbers higher than 1.0 to make test timeouts more lax,
          |use numbers lower than 1.0 to make test timeouts more strict.""".stripMargin
      )
      .discard

    opt[Int](name = "concurrent-test-runs")
      .optional()
      .action((v, c) => c.copy(concurrentTestRuns = v))
      .text(
        "Number of tests to run concurrently. Defaults to the number of available processors or 4, whichever is smaller."
      )
      .discard

    opt[Unit]("verbose")
      .abbr("v")
      .action((_, c) => c.copy(verbose = true))
      .text("Prints full stack traces on failures.")
      .discard

    opt[Unit]("report-problems-only")
      .action((_, c) => c.copy(reportOnFailuresOnly = true))
      .text("Prints report only if there are failures.")
      .discard

    opt[Unit]("must-fail")
      .action((_, c) => c.copy(mustFail = true))
      .text(
        """Reverse success status logic of the tool. Use this flag if you expect one or
          |more or the scenario tests to fail. If enabled, the tool will succeed when at
          |least one test fails, and it will fail when all tests succeed. Defaults to
          |false.""".stripMargin
      )
      .discard

    opt[Unit]('x', "extract")
      .action((_, c) => c.copy(extract = true))
      .text(
        """Extract a DAR necessary to test a Daml ledger and exit without running tests.
          |The DAR needs to be manually loaded into a Daml ledger for the tool to work.""".stripMargin
      )
      .discard

    opt[Seq[String]]("exclude")
      .action((ex, c) => c.copy(excluded = c.excluded ++ ex))
      .unbounded()
      .text(
        """A comma-separated list of exclusion prefixes. Tests whose name start with
          |any of the given prefixes will be skipped. Can be specified multiple times,
          |i.e. `--exclude=a,b` is the same as `--exclude=a --exclude=b`.""".stripMargin
      )
      .discard

    opt[Seq[String]]("include")
      .action((inc, c) => c.copy(included = c.included ++ inc))
      .unbounded()
      .text(
        """A comma-separated list of inclusion prefixes. If not specified,
          |all default tests are included. If specified, only tests that match at least one
          |of the given inclusion prefixes (and none of the given exclusion prefixes) will be run.
          |Can be specified multiple times, i.e. `--include=a,b` is the same as `--include=a --include=b`.
          |Mutually exclusive with `--additional`.""".stripMargin
      )
      .discard

    opt[Seq[String]]("additional")
      .action((additional, c) => c.copy(additional = c.additional ++ additional))
      .hidden()
      .unbounded()
      .text(
        """A comma-separated list of additional prefixes. If specified, also tests that match at least one
          |of the given inclusion prefixes (and none of the given exclusion prefixes) will be run.
          |Can be specified multiple times, i.e. `--additional=a,b` is the same as `--additional=a --additional=b`.
          |Mutually exclusive with `--include`.""".stripMargin
      )
      .discard

    opt[Unit]("shuffle-participants")
      .action((_, c) => c.copy(shuffleParticipants = true))
      .text(
        """Shuffle the list of participants used in a test.
          |By default participants are used in the order they're given.""".stripMargin
      )
      .discard

    opt[Unit]("no-wait-for-parties")
      .action((_, c) => c.copy(partyAllocation = PartyAllocationConfiguration.ClosedWorld))
      .text("Do not wait for parties to be allocated on all participants.")
      .hidden()
      .discard

    opt[Unit]("open-world")
      .action((_, c) => c.copy(partyAllocation = PartyAllocationConfiguration.OpenWorld))
      .text(
        """Do not allocate parties explicitly.
          |Instead, expect the ledger to allocate parties dynamically.
          |Party names must be their hints.""".stripMargin
      )
      .discard

    opt[Unit]("list")
      .action((_, c) => c.copy(listTestSuites = true))
      .text(
        """Lists all available test suites that can be used in the include and exclude options.
          |Test names always start with their suite name, so using the suite name as a prefix
          |matches all tests in a given suite.""".stripMargin
      )
      .discard

    opt[Unit]("list-all")
      .action((_, c) => c.copy(listTests = true))
      .text("Lists all available tests that can be used in the include and exclude options.")
      .discard

    opt[Unit]("version")
      .optional()
      .action { (_, _) =>
        println(BuildInfo.version)
        sys.exit(0)
      }
      .text("Prints the version on stdout and exit.")
      .discard

    opt[FiniteDuration]("ledger-clock-granularity")(
      oneOfRead(Read.finiteDurationRead, Read.intRead.map(_.millis))
    )
      .optional()
      .action((x, c) => c.copy(ledgerClockGranularity = x))
      .text(
        """Specify the largest interval that you will see between clock ticks
          |on the ledger under test. The default is \"1s\" (1 second).""".stripMargin
      )
      .discard

    opt[String]("skip-dar-names-upload")
      .optional()
      .action((skipPattern, c) =>
        c.copy(skipDarNamesPattern = Option.when(skipPattern.nonEmpty)(new Regex(skipPattern)))
      )
      .text("Skip uploading DARs whose names match the provided pattern")
      .discard

    opt[Int](name = "connected-synchronizers")
      .optional()
      .action((v, c) => c.copy(connectedSynchronizers = v))
      .text(
        "Number of synchronizers that each participant is connected to"
      )
      .discard

    checkConfig(c =>
      if (c.included.nonEmpty && c.additional.nonEmpty)
        failure("`--include` and `--additional` are mutually exclusive")
      else
        success
    ).discard

    help("help").text("Prints this usage text").discard
  }

  private def oneOfRead[T](readersHead: Read[T], readersTail: Read[T]*): Read[T] = Read.reads {
    str =>
      val results =
        (readersHead #:: LazyList(readersTail*)).map(reader => Try(reader.reads(str)))
      results.find(_.isSuccess) match {
        case Some(value) => value.get
        case None => results.head.get // throw the first failure
      }
  }
}
