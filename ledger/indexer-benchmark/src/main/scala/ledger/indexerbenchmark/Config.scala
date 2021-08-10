// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import java.time.Duration

import com.daml.lf.data.Ref
import com.daml.metrics.MetricsReporter
import com.daml.platform.configuration.Readers._
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import scopt.OptionParser

/** @param updateCount The number of updates to process.
  * @param updateSource The name of the source of state updates.
  * @param waitForUserInput If enabled, the app will wait for user input after the benchmark has finished,
  *                         but before cleaning up resources.
  */
case class Config(
    updateCount: Option[Long],
    updateSource: String,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    indexerConfig: IndexerConfig,
    waitForUserInput: Boolean,
)

object Config {
  private[indexerbenchmark] val DefaultConfig: Config = Config(
    updateCount = None,
    updateSource = "",
    metricsReporter = None,
    metricsReportingInterval = Duration.ofSeconds(1),
    indexerConfig = IndexerConfig(
      participantId = Ref.ParticipantId.assertFromString("IndexerBenchmarkParticipant"),
      jdbcUrl = "",
      startupMode = IndexerStartupMode.MigrateAndStart,
      enableAppendOnlySchema = true,
    ),
    waitForUserInput = false,
  )

  private[this] val Parser: OptionParser[Config] =
    new OptionParser[Config]("indexer-benchmark") {
      head("Indexer Benchmark")
      note(
        s"Measures the performance of the indexer"
      )
      help("help")
      arg[String]("source")
        .text("The name of the source of state updates.")
        .action((value, config) => config.copy(updateSource = value))

      opt[Int]("indexer-input-mapping-parallelism")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(inputMappingParallelism = value))
        )
      opt[Int]("indexer-ingestion-parallelism")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(ingestionParallelism = value))
        )
      opt[Long]("indexer-submission-batch-size")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(submissionBatchSize = value))
        )
      opt[Int]("indexer-tailing-rate-limit-per-second")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(tailingRateLimitPerSecond = value))
        )
      opt[Long]("indexer-batch-within-millis")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(batchWithinMillis = value))
        )
      opt[Boolean]("indexer-enable-compression")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(enableCompression = value))
        )
      opt[Int]("indexer-max-input-buffer-size")
        .text("[TODO] Sets the corresponding indexer parameter.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(maxInputBufferSize = value))
        )

      opt[String]("jdbc-url")
        .text(
          "The JDBC URL of the index database. Default: the benchmark will run against an ephemeral Postgres database."
        )
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(jdbcUrl = value))
        )

      opt[Long]("update-count")
        .text(
          "The maximum number of updates to process. Default: consume the entire input stream once (the app will not terminate if the input stream is endless)."
        )
        .action((value, config) => config.copy(updateCount = Some(value)))

      opt[Boolean]("wait-for-user-input")
        .text(
          "If enabled, the app will wait for user input after the benchmark has finished, but before cleaning up resources. Use to inspect the contents of an ephemeral index database."
        )
        .action((value, config) => config.copy(waitForUserInput = value))

      opt[MetricsReporter]("metrics-reporter")
        .optional()
        .text(s"Start a metrics reporter. ${MetricsReporter.cliHint}")
        .action((reporter, config) => config.copy(metricsReporter = Some(reporter)))

      opt[Duration]("metrics-reporting-interval")
        .optional()
        .text("Set metric reporting interval.")
        .action((interval, config) => config.copy(metricsReportingInterval = interval))
    }

  def parse(args: collection.Seq[String]): Option[Config] =
    Parser.parse(args, Config.DefaultConfig)
}
