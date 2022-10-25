// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import com.daml.lf.data.Ref
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.configuration.Readers._
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import scopt.OptionParser

import java.time.Duration
import com.daml.metrics.api.reporters.MetricsReporter

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
    indexServiceConfig: IndexServiceConfig,
    indexerConfig: IndexerConfig,
    waitForUserInput: Boolean,
    minUpdateRate: Option[Long],
    participantId: Ref.ParticipantId,
    dataSource: ParticipantDataSourceConfig,
)

object Config {
  private[indexerbenchmark] val DefaultConfig: Config = Config(
    updateCount = None,
    updateSource = "",
    metricsReporter = None,
    metricsReportingInterval = Duration.ofSeconds(1),
    indexServiceConfig = IndexServiceConfig(),
    indexerConfig = IndexerConfig(
      startupMode = IndexerStartupMode.MigrateAndStart()
    ),
    waitForUserInput = false,
    minUpdateRate = None,
    participantId = Ref.ParticipantId.assertFromString("IndexerBenchmarkParticipant"),
    dataSource = ParticipantDataSourceConfig(""),
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
        .text("Sets the value of IndexerConfig.inputMappingParallelism.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(inputMappingParallelism = value))
        )
      opt[Int]("indexer-ingestion-parallelism")
        .text("Sets the value of IndexerConfig.ingestionParallelism.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(ingestionParallelism = value))
        )
      opt[Int]("indexer-batching-parallelism")
        .text("Sets the value of IndexerConfig.batchingParallelism.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(batchingParallelism = value))
        )
      opt[Long]("indexer-submission-batch-size")
        .text("Sets the value of IndexerConfig.submissionBatchSize.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(submissionBatchSize = value))
        )
      opt[Boolean]("indexer-enable-compression")
        .text("Sets the value of IndexerConfig.enableCompression.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(enableCompression = value))
        )
      opt[Int]("indexer-max-input-buffer-size")
        .text("Sets the value of IndexerConfig.maxInputBufferSize.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(maxInputBufferSize = value))
        )

      opt[String]("jdbc-url")
        .text(
          "The JDBC URL of the index database. Default: the benchmark will run against an ephemeral Postgres database."
        )
        .action((value, config) => config.copy(dataSource = ParticipantDataSourceConfig(value)))

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

      opt[Long]("min-update-rate")
        .text(
          "Minimum value of the processed updates per second. If not satisfied the application will report an error."
        )
        .action((value, config) => config.copy(minUpdateRate = Some(value)))

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
