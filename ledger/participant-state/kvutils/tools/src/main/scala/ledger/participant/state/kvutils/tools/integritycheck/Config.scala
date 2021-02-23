// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.nio.file.{Path, Paths}

import com.daml.ledger.participant.state.kvutils.export.LedgerDataExporter
import scopt.OptionParser

case class Config(
                   exportFilePath: Path,
                   performByteComparison: Boolean,
                   sortWriteSet: Boolean,
                   indexOnly: Boolean = false,
                   reportMetrics: Boolean = false,
                   jdbcUrl: Option[String] = None,
                   indexerPerfTest: Boolean = false,
                   deserMappingPar: Int = 1,
                   deserMappingBatchSize: Int = 1,
                   inputMappingParallelism: Int = 1,
                   ingestionParallelism: Int = 1,
                   submissionBatchSize: Long = 1,
                   tailingRateLimitPerSecond: Int = 2,
                   batchWithinMillis: Long = 10,
) {
  def exportFileName: String = exportFilePath.getFileName.toString
}

object Config {
  private[integritycheck] val ParseInput: Config = Config(
    exportFilePath = null,
    performByteComparison = true,
    sortWriteSet = false,
  )

  private implicit val `Read Path`: scopt.Read[Path] = scopt.Read.reads(Paths.get(_))

  private val Parser: OptionParser[Config] =
    new OptionParser[Config]("integrity-checker") {
      head("kvutils Integrity Checker")
      note(
        s"You can produce a ledger export on a kvutils ledger by setting ${LedgerDataExporter.EnvironmentVariableName}=/path/to/file${System.lineSeparator}"
      )
      help("help")
      arg[Path]("PATH")
        .text("The path to the ledger export file (uncompressed).")
        .action((exportFilePath, config) => config.copy(exportFilePath = exportFilePath))
      opt[Unit]("skip-byte-comparison")
        .text("Skips the byte-for-byte comparison. Useful when comparing behavior across versions.")
        .action((_, config) => config.copy(performByteComparison = false))
      opt[Unit]("sort-write-set")
        .text(
          "Sorts the computed write set. Older exports sorted before writing. Newer versions order them intentionally."
        )
        .action((_, config) => config.copy(sortWriteSet = true))
      opt[Unit]("index-only")
        .text(
          "Run only the indexing step of the integrity checker (useful tp benchmark the indexer)."
        )
        .action((_, config) => config.copy(indexOnly = true))
      opt[String]("jdbc-url")
        .text("External JDBC URL (useful for running against PostgreSQL).")
        .action((jdbcUrl, config) => config.copy(jdbcUrl = Some(jdbcUrl)))
      opt[Unit]("report-metrics")
        .text("Print all registered metrics.")
        .action((_, config) => config.copy(reportMetrics = true))
      opt[Unit]("indexer-perf-test")
        .text("No validation: only runs indexer performance test with streaming from export.")
        .action((_, config) => config.copy(indexerPerfTest = true))

    }

  def parse(args: collection.Seq[String]): Option[Config] =
    Parser.parse(args, Config.ParseInput)
}
