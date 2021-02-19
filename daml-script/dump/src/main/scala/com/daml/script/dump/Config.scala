// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.nio.file.Path
import java.io.File

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset

final case class Config(
    ledgerHost: String,
    ledgerPort: Int,
    parties: List[String],
    start: LedgerOffset,
    end: LedgerOffset,
    outputPath: Path,
    sdkVersion: String,
    damlScriptLib: String,
)

object Config {
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

  private val parser = new scopt.OptionParser[Config]("script-dump") {
    opt[String]("host")
      .required()
      .action((x, c) => c.copy(ledgerHost = x))
    opt[Int]("port")
      .required()
      .action((x, c) => c.copy(ledgerPort = x))
    opt[String]("party")
      .required()
      .unbounded()
      .action((x, c) => c.copy(parties = x :: c.parties))
    opt[String]("start")
      .optional()
      .action((x, c) => c.copy(start = parseLedgerOffset(x)))
      .text(
        "The transaction offset (exclusive) for the start position of the dump. Optional, by default the dump includes the beginning of the ledger."
      )
    opt[String]("end")
      .optional()
      .action((x, c) => c.copy(start = parseLedgerOffset(x)))
      .text(
        "The transaction offset (inclusive) for the end position of the dump. Optional, by default the dump includes the current end of the ledger."
      )
    opt[File]('o', "output")
      .required()
      .action((x, c) => c.copy(outputPath = x.toPath))
    opt[String]("sdk-version")
      .required()
      .action((x, c) => c.copy(sdkVersion = x))
  }

  private val Empty = Config(
    ledgerHost = "",
    ledgerPort = -1,
    parties = List(),
    start = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
    end = LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END)),
    outputPath = null,
    sdkVersion = "",
    damlScriptLib = "daml-script",
  )
}
