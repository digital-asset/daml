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

    opt[Int]('p', "target-port")
      .action((x, c) => c.copy(port = x))
      .text("Server port of the Ledger API endpoint to test. Defaults to 6865.")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("Server host of the Ledger API endpoint to test. Defaults to localhost.")

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

    opt[Double](name = "timeout-scale-factor")
      .optional()
      .action((v, c) => c.copy(timeoutScaleFactor = v))
      .text("""Scale factor for timeouts used in testing. Useful to tune timeouts
          |depending on the environment and the Ledger implementation under test.
          |Defaults to 1.0. Use numbers higher than 1.0 to make test timeouts more lax,
          |use numbers lower than 1.0 to make test timeouts more strict.""".stripMargin)

    opt[Unit]("verbose")
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
      .text("""Extract a DAR necessary to test a DAML ledger and exit without running tests.
              |The DAR needs to be manually loaded into a DAML ledger for the tool to work.""".stripMargin)

  }

  def parse(args: Array[String]): Option[Config] =
    argParser.parse(args, Config.default)
}
