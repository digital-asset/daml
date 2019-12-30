// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.ledger.api.server.damlonx.reference.v2.cli

import java.io.File

import com.daml.ledger.api.server.damlonx.reference.v2.Config
import com.digitalasset.daml.lf.data.Ref
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

  private def cmdArgParser(binaryName: String, description: String) =
    new scopt.OptionParser[Config](binaryName) {
      head(description)
      opt[Int]("port")
        .optional()
        .action((p, c) => c.copy(port = p))
        .text("Server port. If not set, a random port is allocated.")
      opt[File]("port-file")
        .optional()
        .action((f, c) => c.copy(portFile = Some(f.toPath)))
        .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")
      opt[String]("pem")
        .optional()
        .text("TLS: The pem file to be used as the private key.")
        .action((path, config) => config.withTlsConfig(c => c.copy(keyFile = Some(new File(path)))))
      opt[String]("crt")
        .optional()
        .text("TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.")
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(keyCertChainFile = Some(new File(path)))))
      opt[String]("cacrt")
        .optional()
        .text("TLS: The crt file to be used as the the trusted root CA.")
        .action((path, config) =>
          config.withTlsConfig(c => c.copy(trustCertCollectionFile = Some(new File(path)))))
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
      opt[(Ref.LedgerString, Int, String)]('P', "extra-participant")
        .optional()
        .unbounded()
        .text("A list of triples in the form `<participant-id>,<port>,<index-jdbc-url>` to spin up multiple nodes backed by the same in-memory ledger")
        .action((e, c) => c.copy(extraParticipants = c.extraParticipants :+ e))
      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .action((f, c) => c.copy(archiveFiles = f :: c.archiveFiles))
        .text("DAR files to load. Scenarios are ignored. The servers starts with an empty ledger by default.")
    }

  def parse(args: Array[String], binaryName: String, description: String): Option[Config] =
    cmdArgParser(binaryName, description).parse(args, Config.default)
}
