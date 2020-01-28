// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File

import com.digitalasset.ledger.api.tls.TlsConfiguration
import scopt.Read
import scopt.Read.{intRead, stringRead}

object Cli {

  private def endpointRead: Read[(String, Int)] = new Read[(String, Int)] {
    val arity = 2
    val reads = { (s: String) =>
      splitAddress(s) match {
        case (k, v) => stringRead.reads(k) -> intRead.reads(v)
      }
    }
  }

  private def splitAddress(s: String): (String, String) =
    s.indexOf(':') match {
      case -1 =>
        throw new IllegalArgumentException("Addresses should be specified as `<host>:<port>`")
      case n: Int => (s.slice(0, n), s.slice(n + 1, s.length))
    }

  private val pemConfig = (path: String, config: Config) =>
    config.copy(
      tlsConfig = config.tlsConfig.fold(
        Some(TlsConfiguration(enabled = true, None, Some(new File(path)), None)),
      )(c => Some(c.copy(keyFile = Some(new File(path))))),
    )

  private val crtConfig = (path: String, config: Config) =>
    config.copy(
      tlsConfig = config.tlsConfig.fold(
        Some(TlsConfiguration(enabled = true, Some(new File(path)), None, None)),
      )(c => Some(c.copy(keyCertChainFile = Some(new File(path))))),
    )

  private val cacrtConfig = (path: String, config: Config) =>
    config.copy(
      tlsConfig = config.tlsConfig.fold(
        Some(TlsConfiguration(enabled = true, None, None, Some(new File(path)))),
      )(c => Some(c.copy(trustCertCollectionFile = Some(new File(path))))),
    )

  private val argParser = new scopt.OptionParser[Config]("ledger-api-test-tool") {
    head("""The Ledger API Test Tool is a command line tool for testing the correctness of
        |ledger implementations based on DAML and Ledger API.""".stripMargin)

    arg[(String, Int)]("[endpoints...]")(endpointRead)
      .action((address, config) => config.copy(participants = config.participants :+ address))
      .unbounded()
      .optional()
      .text("""Addresses of the participants to test, specified as `<host>:<port>`.""")

    // FIXME Make client_server_test more flexible and remove this deprecated option
    opt[String]("target-port")
      .optional()
      .text("DEPRECATED: this option is no longer used and has no effect")

    opt[String]("pem")
      .optional()
      .text("TLS: The pem file to be used as the private key. Applied to all endpoints.")
      .action(pemConfig)

    opt[String]("crt")
      .optional()
      .text(
        "TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set. Applied to all endpoints.",
      )
      .action(crtConfig)

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the the trusted root CA. Applied to all endpoints.")
      .action(cacrtConfig)

    opt[Double](name = "command-submission-ttl-scale-factor")
      .optional()
      .action((v, c) => c.copy(commandSubmissionTtlScaleFactor = v))
      .text("""Scale factor for time-to-live of commands sent for ledger processing
              |(captured as Maximum Record Time in submitted transactions) for
              |all test suites. Regardless the output of multiplying by this factor
              |the TTL will always be clipped by the minimum and maximum value as defined
              |by the LedgerConfigurationService, with the maximum being the default
              |(which means that any value above 1.0 won't have any effect.""".stripMargin)

    opt[Double](name = "timeout-scale-factor")
      .optional()
      .action((v, c) => c.copy(timeoutScaleFactor = v))
      .text("""Scale factor for timeouts used in all test suites. Useful to tune timeouts
              |depending on the environment and the Ledger implementation under test.
              |Defaults to 1.0. Use numbers higher than 1.0 to make test timeouts more lax,
              |use numbers lower than 1.0 to make test timeouts more strict.""".stripMargin)

    opt[Double](name = "load-scale-factor")
      .optional()
      .action((v, c) => c.copy(loadScaleFactor = v))
      .text("""Scale factor for the load used in scale test suites. Useful to adapt the load
              |depending on the environment and the Ledger implementation under test.
              |Defaults to 1.0. Use numbers higher than 1.0 to increase the load,
              |use numbers lower than 1.0 to decrease the load.""".stripMargin)

    opt[Int](name = "concurrent-test-runs")
      .optional()
      .action((v, c) => c.copy(concurrentTestRuns = v))
      .text("""Number of tests to run concurrently. Defaults to the number of available
              |processors.""".stripMargin)

    opt[Unit]("verbose")
      .abbr("v")
      .action((_, c) => c.copy(verbose = true))
      .text("Prints full stacktraces on failures.")

    opt[Unit]("must-fail")
      .action((_, c) => c.copy(mustFail = true))
      .text("""Reverse success status logic of the tool. Use this flag if you expect one or
              |more or the scenario tests to fail. If enabled, the tool will succeed when at
              |least one test fails, and it will fail when all tests succeed. Defaults to
              |false.""".stripMargin)

    opt[Unit]('x', "extract")
      .action((_, c) => c.copy(extract = true))
      .text(
        """Extract a DAR necessary to test a DAML ledger and exit without running tests.
              |The DAR needs to be manually loaded into a DAML ledger for the tool to work.""".stripMargin,
      )

    opt[Seq[String]]("exclude")
      .action((ex, c) => c.copy(excluded = c.excluded ++ ex))
      .unbounded()
      .text(
        """A comma-separated list of tests that should NOT be run. By default, no tests are excluded.""",
      )

    opt[Seq[String]]("include")
      .action((inc, c) => c.copy(included = c.included ++ inc))
      .unbounded()
      .text("""A comma-separated list of tests that should be run.""")

    opt[Unit]("all-tests")
      .action((_, c) => c.copy(allTests = true))
      .text("""Run all default and optional tests. Respects the --exclude flag.""")

    opt[Unit]("list")
      .action((_, c) => c.copy(listTests = true))
      .text("""Lists all available tests that can be used in the include and exclude options.""")

    help("help").text("Prints this usage text")

  }

  def parse(args: Array[String]): Option[Config] =
    argParser.parse(args, Config.default)
}
