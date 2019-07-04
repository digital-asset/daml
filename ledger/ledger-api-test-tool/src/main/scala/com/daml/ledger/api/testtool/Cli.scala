// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File

import com.digitalasset.ledger.api.tls.TlsConfiguration

object Cli {

  private val pemConfig = (path: String, config: Config) =>
    config.copy(
      tlsConfig = config.tlsConfig.fold(
        Some(TlsConfiguration(enabled = true, None, Some(new File(path)), None)))(c =>
        Some(c.copy(keyFile = Some(new File(path))))))

  private val crtConfig = (path: String, config: Config) =>
    config.copy(
      tlsConfig = config.tlsConfig.fold(
        Some(TlsConfiguration(enabled = true, Some(new File(path)), None, None)))(c =>
        Some(c.copy(keyCertChainFile = Some(new File(path))))))

  private val cacrtConfig = (path: String, config: Config) =>
    config.copy(
      tlsConfig = config.tlsConfig.fold(
        Some(TlsConfiguration(enabled = true, None, None, Some(new File(path)))))(c =>
        Some(c.copy(trustCertCollectionFile = Some(new File(path))))))

  private val argParser = new scopt.OptionParser[Config]("ledger-api-test-tool") {
    head("""The Ledger API Test Tool is a command line tool for testing the correctness of
        |ledger implementations based on DAML and Ledger API.""".stripMargin)

    help("help").text("prints this usage text")

    opt[(String, String)]("mapping")
      .unbounded()
      .action({case ((party, hostport), c) => {
        val (host, port) = hostport.split(":") match { case Array(h, p) => (h, Integer.parseInt(p)) }
        c.copy(mapping = c.mapping + ((party -> ((host, port)))))
      }})
      .text(s"Ledger API server mapping. Defaults to a single host and port for all parties. TLS configuration is not implemented for multi-endpoint testing.")

    opt[Int]('p', "target-port")
      .action((x, c) => c.copy(port = x))
      .text(
        "Server port of the Ledger API endpoint to test, if no mapping provided. Defaults to 6865.")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text(
        "Server host of the Ledger API endpoint to test, if no mapping provided. Defaults to localhost.")

    opt[String]("pem")
      .optional()
      .text("TLS: The pem file to be used as the private key.")
      .action(pemConfig)

    opt[String]("crt")
      .optional()
      .text("TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.")
      .action(crtConfig)

    opt[String]("cacrt")
      .optional()
      .text("TLS: The crt file to be used as the the trusted root CA.")
      .action(cacrtConfig)

    opt[Double](name = "command-submission-ttl-scale-factor")
      .optional()
      .action((v, c) => c.copy(commandSubmissionTtlScaleFactor = v))
      .text("""Scale factor for time-to-live of commands sent for ledger processing
              |(captured as Maximum Record Time in submitted transactions) for
              |"SemanticTests" suite. Useful to tune Maximum Record Time depending on
              |the environment and the Ledger implementation under test. Defaults to
              |1.0. Use numbers higher than 1.0 to make timeouts more lax, use
              |numbers lower than 1.0 to make timeouts more strict.""".stripMargin)

    opt[Double](name = "timeout-scale-factor")
      .optional()
      .action((v, c) => c.copy(timeoutScaleFactor = v))
      .text("""Scale factor for timeouts used in all test suites. Useful to tune timeouts
          |depending on the environment and the Ledger implementation under test.
          |Defaults to 1.0. Use numbers higher than 1.0 to make test timeouts more lax,
          |use numbers lower than 1.0 to make test timeouts more strict.""".stripMargin)

    opt[Unit]("verbose")
      .abbr("v")
      .action((_, c) => c.copy(verbose = true))
      .text("Prints full stacktraces on failures.")

    opt[Unit]("stable-party-identifiers")
      .abbr("sp")
      .action((_, c) => c.copy(uniquePartyIdentifiers = false))
      .text("""Use the same party identifiers for each run. By default
          |those are randomized for each execution of the tool to ensure that
          |the tests are not being failed by command and party deduplication mechanisms.""".stripMargin)

    opt[Unit]("stable-command-identifiers")
      .abbr("sc")
      .action((_, c) => c.copy(uniqueCommandIdentifiers = false))
      .text("""Use the same command identifiers for each run. By default
          |those are randomized for each execution of the tool to ensure that
          |the tests are not being failed by command and party deduplication mechanisms.""".stripMargin)

    opt[Unit]("must-fail")
      .action((_, c) => c.copy(mustFail = true))
      .text("""Reverse success status logic of the tool. Use this flag if you expect one or
          |more or the scenario tests to fail. If enabled, the tool will succeed when at
          |least one test fails, and it will fail when all tests succeed. Defaults to
          |false.""".stripMargin)

    opt[Unit]('x', "extract")
      .action((_, c) => c.copy(extract = true))
      .text("""Extract a DAR necessary to test a DAML ledger and exit without running tests.
              |The DAR needs to be manually loaded into a DAML ledger for the tool to work.""".stripMargin)

    opt[Seq[String]]("exclude")
      .action((ex, c) => c.copy(excluded = c.excluded ++ ex))
      .unbounded()
      .text("""A comma-separated list of tests that should NOT be run. By default, no tests are excluded.""")

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

  }

  def parse(args: Array[String]): Option[Config] =
    argParser.parse(args, Config.default)
}
