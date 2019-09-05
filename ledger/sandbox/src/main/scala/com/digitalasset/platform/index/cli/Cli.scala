// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index.cli

import java.io.File

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.index.config.Config
import scopt.Read

object Cli {

  private implicit val ledgerStringRead: Read[Ref.LedgerString] =
    Read.stringRead.map(Ref.LedgerString.assertFromString)

  private implicit def tripleRead[A, B, C](
      implicit readA: Read[A],
      readB: Read[B],
      readC: Read[C]): Read[(A, B, C)] =
    Read.seqRead[String].map {
      case Seq(a, b, c) => (readA.reads(a), readB.reads(b), readC.reads(c))
      case a => throw new RuntimeException(s"Expected a comma-separated triple, got '$a'")
    }

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

  private def cmdArgParser(
      binaryName: String,
      description: String,
      allowExtraParticipants: Boolean) =
    new scopt.OptionParser[Config](binaryName) {
      head(description)
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
      opt[Int]("maxInboundMessageSize")
        .action((x, c) => c.copy(maxInboundMessageSize = x))
        .text(
          s"Max inbound message size in bytes. Defaults to ${Config.DefaultMaxInboundMessageSize}.")
      opt[String]("jdbc-url")
        .text("The JDBC URL to the postgres database used for the indexer and the index")
        .action((u, c) => c.copy(jdbcUrl = u))
      opt[Ref.LedgerString]("participant-id")
        .optional()
        .text("The participant id given to all components of a ledger api server")
        .action((p, c) => c.copy(participantId = p))
      if (allowExtraParticipants) {
        opt[(Ref.LedgerString, Int, String)]('P', "extra-participant")
          .optional()
          .unbounded()
          .text("A list of triples in the form `<participant-id>,<port>,<index-jdbc-url>` to spin up multiple nodes backed by the same in-memory ledger")
          .action((e, c) => c.copy(extraPartipants = c.extraPartipants :+ e))
      }
      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .action((f, c) => c.copy(archiveFiles = f :: c.archiveFiles))
        .text("DAR files to load. Scenarios are ignored. The servers starts with an empty ledger by default.")
    }

  def parse(
      args: Array[String],
      binaryName: String,
      description: String,
      allowExtraParticipants: Boolean = false): Option[Config] =
    cmdArgParser(binaryName, description, allowExtraParticipants).parse(args, Config.default)
}
