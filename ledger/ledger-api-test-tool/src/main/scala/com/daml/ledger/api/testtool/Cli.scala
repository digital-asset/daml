// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.testtool.tests.Tests
import com.daml.ledger.api.tls.TlsVersion
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import scopt.{OptionParser, Read}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.collection.compat.immutable.LazyList
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

object Cli {

  private val Name = "ledger-api-test-tool"

  private def reportUsageOfDeprecatedOption[B](option: String) = { (_: Any, config: B) =>
    System.err.println(
      s"WARNING: $option has been deprecated and will be removed in a future version"
    )
    config
  }

  private def endpointRead: Read[(String, Int)] = new Read[(String, Int)] {
    val arity = 2
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

  private[this] implicit val pathRead: Read[Path] = Read.reads(Paths.get(_))

  private[this] implicit val tlsVersionRead: Read[TlsVersion] = Read.reads {
    case "1" => TlsVersion.V1
    case "1.1" => TlsVersion.V1_1
    case "1.2" => TlsVersion.V1_2
    case "1.3" => TlsVersion.V1_3
  }

  private val argParser: OptionParser[Config] = new scopt.OptionParser[Config](Name) {
    private def invalidPerformanceTestName[A](name: String): Either[String, Unit] =
      failure(s"$name is not a valid performance test name. Use `--list` to see valid names.")

    head("""The Ledger API Test Tool is a command line tool for testing the correctness of
        |ledger implementations based on Daml and Ledger API.""".stripMargin)

    arg[(String, Int)]("[endpoints...]")(endpointRead)
      .action((address, config) =>
        config.copy(participantsEndpoints = config.participantsEndpoints :+ address)
      )
      .unbounded()
      .optional()
      .text("""Addresses of the participants servers to test, specified as `<host>:<port>`.""")

    opt[Int]("max-connection-attempts")
      .action((maxConnectionAttempts, config) =>
        config.copy(maxConnectionAttempts = maxConnectionAttempts)
      )
      .optional()
      .text("Number of connection attempts to the participants. Applied to all endpoints.")

    // FIXME Make client_server_test more flexible and remove this deprecated option
    opt[String]("target-port")
      .optional()
      .text("DEPRECATED: this option is no longer used and has no effect")
      .action(reportUsageOfDeprecatedOption("--target-port"))
      .hidden()

    opt[String]("pem")
      .optional()
      .text("TLS: The pem file to be used as the private key. Applied to all endpoints.")
      .action { (path: String, config: Config) =>
        config.withTlsConfig(_.copy(keyFile = Some(new File(path))))
      }

    opt[String]("crt")
      .optional()
      .text(
        "TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set. Applied to all endpoints."
      )
      .action { (path: String, config: Config) =>
        config.withTlsConfig(_.copy(keyCertChainFile = Some(new File(path))))
      }

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the trusted root CA. Applied to all endpoints.")
      .action { (path: String, config: Config) =>
        config.withTlsConfig(_.copy(trustCertCollectionFile = Some(new File(path))))
      }

    opt[TlsVersion]("tls-version")
      .optional()
      .text("TLS: TLS version to enable.")
      .action { (tlsVersion: TlsVersion, config: Config) =>
        config.withTlsConfig(_.copy(clientProtocolVersion = Some(tlsVersion)))
      }

    opt[Double](name = "timeout-scale-factor")
      .optional()
      .action((v, c) => c.copy(timeoutScaleFactor = v))
      .text("""Scale factor for timeouts used in all test suites. Useful to tune timeouts
          |depending on the environment and the Ledger implementation under test.
          |Defaults to 1.0. Use numbers higher than 1.0 to make test timeouts more lax,
          |use numbers lower than 1.0 to make test timeouts more strict.""".stripMargin)

    opt[String](name = "load-scale-factor")
      .optional()
      .text("DEPRECATED: this option is no longer used and has no effect")
      .action(reportUsageOfDeprecatedOption("--load-scale-factor"))
      .hidden()

    opt[Int](name = "concurrent-test-runs")
      .optional()
      .action((v, c) => c.copy(concurrentTestRuns = v))
      .text("Number of tests to run concurrently. Defaults to the number of available processors")

    opt[Unit]("verbose")
      .abbr("v")
      .action((_, c) => c.copy(verbose = true))
      .text("Prints full stack traces on failures.")

    opt[Unit]("must-fail")
      .action((_, c) => c.copy(mustFail = true))
      .text("""Reverse success status logic of the tool. Use this flag if you expect one or
          |more or the scenario tests to fail. If enabled, the tool will succeed when at
          |least one test fails, and it will fail when all tests succeed. Defaults to
          |false.""".stripMargin)

    opt[Unit]('x', "extract")
      .action((_, c) => c.copy(extract = true))
      .text(
        """Extract a DAR necessary to test a Daml ledger and exit without running tests.
          |The DAR needs to be manually loaded into a Daml ledger for the tool to work.""".stripMargin
      )

    opt[Seq[String]]("exclude")
      .action((ex, c) => c.copy(excluded = c.excluded ++ ex))
      .unbounded()
      .text(
        """A comma-separated list of exclusion prefixes. Tests whose name start with any of the given prefixes will be skipped. Can be specified multiple times, i.e. `--exclude=a,b` is the same as `--exclude=a --exclude=b`."""
      )

    opt[Seq[String]]("include")
      .action((inc, c) => c.copy(included = c.included ++ inc))
      .unbounded()
      .text(
        """A comma-separated list of inclusion prefixes. If not specified, all default tests are included. If specified, only tests that match at least one of the given inclusion prefixes (and none of the given exclusion prefixes) will be run. Can be specified multiple times, i.e. `--include=a,b` is the same as `--include=a --include=b`."""
      )

    opt[Seq[String]]("perf-tests")
      .validate(_.find(!Tests.PerformanceTestsKeys(_)).fold(success)(invalidPerformanceTestName))
      .action((inc, c) => c.copy(performanceTests = c.performanceTests ++ inc))
      .unbounded()
      .text("""A comma-separated list of performance tests that should be run.""")

    opt[Path]("perf-tests-report")
      .action((inc, c) => c.copy(performanceTestsReport = Some(inc)))
      .optional()
      .text(
        "The path of the benchmark report file produced by performance tests (default: stdout)."
      )

    opt[Unit]("all-tests")
      .text("DEPRECATED: All tests are always run by default.")
      .action(reportUsageOfDeprecatedOption("--all-tests"))
      .hidden()

    opt[Unit]("shuffle-participants")
      .action((_, c) => c.copy(shuffleParticipants = true))
      .text("""Shuffle the list of participants used in a test.
          |By default participants are used in the order they're given.""".stripMargin)

    opt[Unit]("no-wait-for-parties")
      .action((_, c) => c.copy(partyAllocation = PartyAllocationConfiguration.ClosedWorld))
      .text("""Do not wait for parties to be allocated on all participants.""")
      .hidden()

    opt[Unit]("open-world")
      .action((_, c) => c.copy(partyAllocation = PartyAllocationConfiguration.OpenWorld))
      .text("""|Do not allocate parties explicitly.
           |Instead, expect the ledger to allocate parties dynamically.
           |Party names must be their hints.""".stripMargin)

    opt[Unit]("list")
      .action((_, c) => c.copy(listTestSuites = true))
      .text(
        """Lists all available test suites that can be used in the include and exclude options. Test names always start with their suite name, so using the suite name as a prefix matches all tests in a given suite."""
      )

    opt[Unit]("list-all")
      .action((_, c) => c.copy(listTests = true))
      .text("""Lists all available tests that can be used in the include and exclude options.""")

    opt[Unit]("version")
      .optional()
      .action((_, _) => {
        println(BuildInfo.Version)
        sys.exit(0)
      })
      .text("Prints the version on stdout and exit.")

    opt[FiniteDuration]("ledger-clock-granularity")(
      oneOfRead(Read.finiteDurationRead, Read.intRead.map(_.millis))
    )
      .optional()
      .action((x, c) => c.copy(ledgerClockGranularity = x))
      .text(
        "Specify the largest interval that you will see between clock ticks on the ledger under test. The default is \"1s\" (1 second)."
      )

    opt[Unit]("skip-dar-upload")
      .optional()
      .action((_, c) => c.copy(uploadDars = false))
      .text("Skip DARs upload into ledger before running tests")

    help("help").text("Prints this usage text")

  }

  def parse(args: Array[String]): Option[Config] =
    argParser.parse(args, Config.default)

  private def oneOfRead[T](readersHead: Read[T], readersTail: Read[T]*): Read[T] = Read.reads {
    str =>
      val results =
        (readersHead #:: LazyList(readersTail: _*)).map(reader => Try(reader.reads(str)))
      results.find(_.isSuccess) match {
        case Some(value) => value.get
        case None => results.head.get // throw the first failure
      }
  }
}
