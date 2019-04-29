// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference

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

  private val cmdArgParser = new scopt.OptionParser[Config]("reference-server") {
    head(
      "A fully compliant DAML Ledger API server backed by an in-memory store.\n" +
        "Due to its lack of persistence it is not meant for production, but to serve as a blueprint for other DAML Ledger API server implementations.")
    opt[Int]("port")
      .optional()
      .action((p, c) => c.copy(port = p))
      .text("Server port. If not set, a random port is allocated.")
    opt[File]("port-file")
      .optional()
      .action((f, c) => c.copy(portFile = Some(f)))
      .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")
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
    opt[Unit]("bad-server")
      .optional()
      .action((_, c) => c.copy(badServer = true))
      .text("Simulate a badly behaving server that returns empty transactions. Defaults to false.")
    arg[File]("<archive>...")
      .unbounded()
      .action((f, c) => c.copy(archiveFiles = f :: c.archiveFiles))
      .text("DAR files to load. Scenarios are ignored. The servers starts with an empty ledger by default.")
  }

  def parse(args: Array[String]): Option[Config] =
    cmdArgParser.parse(args, Config.default)
}
