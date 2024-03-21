// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.nio.file.{Path, Paths}
import java.io.File

import com.daml.auth.TokenHolder
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.tls.{TlsConfiguration, TlsConfigurationCli}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset

final case class Config(
    ledgerHost: String,
    ledgerPort: Int,
    tlsConfig: TlsConfiguration,
    accessToken: Option[TokenHolder],
    partyConfig: PartyConfig,
    start: LedgerOffset,
    end: LedgerOffset,
    exportType: Option[ExportType],
    maxInboundMessageSize: Int,
)

sealed trait ExportType
final case class ExportScript(
    acsBatchSize: Int,
    setTime: Boolean,
    outputPath: Path,
    sdkVersion: String,
    damlScriptLib: String,
) extends ExportType

// This is a product rather than a sum, so that we can raise
// an error of both --party and --all-parties is configured.
final case class PartyConfig(
    allParties: Boolean,
    parties: Seq[Party],
)

object Config {
  val DefaultMaxInboundMessageSize: Int = 4194304

  def parse(args: Array[String]): Option[Config] =
    parser.parse(args, Empty)

  private def parseLedgerOffset(s: String): LedgerOffset = LedgerOffset {
    s match {
      case s if s == "begin" =>
        LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
      case s if s == "end" => LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)
      case s => LedgerOffset.Value.Absolute(s)
    }
  }

  private val parser = new scopt.OptionParser[Config]("script-export") {
    help("help")
      .text("Show this help message.")
    opt[String]("host")
      .required()
      .action((x, c) => c.copy(ledgerHost = x))
      .text("Daml ledger host to connect to.")
    opt[Int]("port")
      .required()
      .action((x, c) => c.copy(ledgerPort = x))
      .text("Daml ledger port to connect to.")
    TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
      c.copy(tlsConfig = f(c.tlsConfig))
    )
    opt[String]("access-token-file")
      .action((f, c) => c.copy(accessToken = Some(new TokenHolder(Paths.get(f)))))
      .text(
        "File from which the access token will be read, required to interact with an authenticated ledger."
      )
    opt[Seq[String]]("party")
      .unbounded()
      .action((x, c) =>
        c.copy(partyConfig =
          c.partyConfig.copy(parties = c.partyConfig.parties ++ Party.subst(x.toList))
        )
      )
      .text(
        "Export ledger state as seen by these parties. " +
          "Pass --party multiple times or use a comma-separated list of party names to specify multiple parties. " +
          "Use either --party or --all-parties but not both."
      )
    opt[Unit]("all-parties")
      .action((_, c) => c.copy(partyConfig = c.partyConfig.copy(allParties = true)))
      .text(
        "Export ledger state as seen by all known parties. " +
          "Use either --party or --all-parties but not both."
      )
    opt[String]("start")
      .optional()
      .action((x, c) => c.copy(start = parseLedgerOffset(x)))
      .text(
        "The transaction offset (exclusive) for the start position of the export. Optional, by default the export includes the beginning of the ledger."
      )
    opt[String]("end")
      .optional()
      .action((x, c) => c.copy(end = parseLedgerOffset(x)))
      .text(
        "The transaction offset (inclusive) for the end position of the export. Optional, by default the export includes the current end of the ledger."
      )
    opt[Int]("max-inbound-message-size")
      .optional()
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .text(
        s"Optional max inbound message size in bytes. Defaults to $DefaultMaxInboundMessageSize"
      )
    cmd("script")
      .action((_, c) => c.copy(exportType = Some(EmptyExportScript)))
      .text("Export ledger state in Daml script format")
      .children(
        opt[Int]("acs-batch-size")
          .optional()
          .action(actionExportScript((x, c) => c.copy(acsBatchSize = x)))
          .validate(x =>
            if (x <= 0) { failure("ACS batch size must be greater than zero") }
            else { success }
          )
          .text("Batch this many create commands into one transaction when recreating the ACS."),
        opt[Boolean]("set-time")
          .optional()
          .action(actionExportScript((x, c) => c.copy(setTime = x)))
          .text(
            "Emit setTime commands to replicate transaction time stamps. Only works on ledgers in static-time mode."
          ),
        opt[File]('o', "output")
          .required()
          .action(actionExportScript((x, c) => c.copy(outputPath = x.toPath)))
          .text("Create the Daml script under this directory prefix."),
        opt[String]("sdk-version")
          .required()
          .action(actionExportScript((x, c) => c.copy(sdkVersion = x)))
          .text("Specify this Daml version in the generated project."),
      )
    checkConfig(c =>
      c.exportType match {
        case None => failure("Must specify export type")
        case Some(_) => success
      }
    )
    checkConfig(c => {
      val pc = c.partyConfig
      if (pc.allParties && pc.parties.nonEmpty) {
        failure("Set either --party or --all-parties but not both at the same time")
      } else if (!pc.allParties && pc.parties.isEmpty) {
        failure("Set one of --party or --all-parties")
      } else {
        success
      }
    })
  }

  val EmptyExportScript = ExportScript(
    outputPath = null,
    sdkVersion = "",
    acsBatchSize = 10,
    setTime = false,
    damlScriptLib = "daml-script",
  )

  private def actionExportScript[T](f: (T, ExportScript) => ExportScript): (T, Config) => Config = {
    case (x, c) => {
      val exportScript = c.exportType match {
        case Some(exportScript: ExportScript) => exportScript
        case _ => EmptyExportScript
      }
      c.copy(exportType = Some(f(x, exportScript)))
    }
  }

  val Empty = Config(
    ledgerHost = "",
    ledgerPort = -1,
    tlsConfig = TlsConfiguration(false, None, None, None),
    accessToken = None,
    partyConfig = PartyConfig(parties = Seq.empty[Party], allParties = false),
    start = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
    end = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
    exportType = None,
    maxInboundMessageSize = DefaultMaxInboundMessageSize,
  )
}
