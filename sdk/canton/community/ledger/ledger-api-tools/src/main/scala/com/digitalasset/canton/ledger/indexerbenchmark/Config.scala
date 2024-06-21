// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.indexerbenchmark

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.config.IndexServiceConfig
import com.digitalasset.canton.platform.config.Readers.*
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import scopt.OptionParser

import java.time.Duration

/** @param updateCount The number of updates to process.
  * @param updateSource The name of the source of state updates.
  * @param waitForUserInput If enabled, the app will wait for user input after the benchmark has finished,
  *                         but before cleaning up resources.
  */
final case class Config(
    updateCount: Option[Long],
    updateSource: String,
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
    metricsReportingInterval = Duration.ofSeconds(1),
    indexServiceConfig = IndexServiceConfig(),
    indexerConfig = IndexerConfig(),
    waitForUserInput = false,
    minUpdateRate = None,
    participantId = Ref.ParticipantId.assertFromString("IndexerBenchmarkParticipant"),
    dataSource = ParticipantDataSourceConfig(""),
  )

  private[this] val Parser: OptionParser[Config] =
    new OptionParser[Config]("indexer-benchmark") {
      head("Indexer Benchmark").discard
      note(
        s"Measures the performance of the indexer"
      ).discard
      help("help").discard
      arg[String]("source")
        .text("The name of the source of state updates.")
        .action((value, config) => config.copy(updateSource = value))
        .discard

      opt[Int]("indexer-input-mapping-parallelism")
        .text("Sets the value of IndexerConfig.inputMappingParallelism.")
        .action((value, config) =>
          config.copy(indexerConfig =
            config.indexerConfig.copy(inputMappingParallelism = NonNegativeInt.tryCreate(value))
          )
        )
        .discard
      opt[Int]("indexer-ingestion-parallelism")
        .text("Sets the value of IndexerConfig.ingestionParallelism.")
        .action((value, config) =>
          config.copy(indexerConfig =
            config.indexerConfig.copy(ingestionParallelism = NonNegativeInt.tryCreate(value))
          )
        )
        .discard
      opt[Int]("indexer-batching-parallelism")
        .text("Sets the value of IndexerConfig.batchingParallelism.")
        .action((value, config) =>
          config.copy(indexerConfig =
            config.indexerConfig.copy(batchingParallelism = NonNegativeInt.tryCreate(value))
          )
        )
        .discard
      opt[Long]("indexer-submission-batch-size")
        .text("Sets the value of IndexerConfig.submissionBatchSize.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(submissionBatchSize = value))
        )
        .discard
      opt[Boolean]("indexer-enable-compression")
        .text("Sets the value of IndexerConfig.enableCompression.")
        .action((value, config) =>
          config.copy(indexerConfig = config.indexerConfig.copy(enableCompression = value))
        )
        .discard
      opt[Int]("indexer-max-input-buffer-size")
        .text("Sets the value of IndexerConfig.maxInputBufferSize.")
        .action((value, config) =>
          config.copy(indexerConfig =
            config.indexerConfig.copy(maxInputBufferSize = NonNegativeInt.tryCreate(value))
          )
        )
        .discard

      opt[String]("jdbc-url")
        .text(
          "The JDBC URL of the index database. Default: the benchmark will run against an ephemeral Postgres database."
        )
        .action((value, config) => config.copy(dataSource = ParticipantDataSourceConfig(value)))
        .discard

      opt[Long]("update-count")
        .text(
          "The maximum number of updates to process. Default: consume the entire input stream once (the app will not terminate if the input stream is endless)."
        )
        .action((value, config) => config.copy(updateCount = Some(value)))
        .discard

      opt[Boolean]("wait-for-user-input")
        .text(
          "If enabled, the app will wait for user input after the benchmark has finished, but before cleaning up resources. Use to inspect the contents of an ephemeral index database."
        )
        .action((value, config) => config.copy(waitForUserInput = value))
        .discard

      opt[Long]("min-update-rate")
        .text(
          "Minimum value of the processed updates per second. If not satisfied the application will report an error."
        )
        .action((value, config) => config.copy(minUpdateRate = Some(value)))
        .discard

      opt[Duration]("metrics-reporting-interval")
        .optional()
        .text("Set metric reporting interval.")
        .action((interval, config) => config.copy(metricsReportingInterval = interval))
        .discard
    }

  def parse(args: collection.Seq[String]): Option[Config] =
    Parser.parse(args, Config.DefaultConfig)
}
