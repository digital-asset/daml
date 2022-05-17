// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import com.codahale.metrics.{MetricRegistry, Snapshot}
import com.daml.metrics.Metrics

class IndexerBenchmarkResult(config: Config, metrics: Metrics, startTime: Long, stopTime: Long) {

  private val duration: Double = (stopTime - startTime).toDouble / 1000000000.0
  private val updates: Long = metrics.daml.parallelIndexer.updates.getCount
  private val updateRate: Double = updates / duration
  private val inputMappingDurationMetric = metrics.registry.timer(
    MetricRegistry.name(metrics.daml.parallelIndexer.inputMapping.executor, "duration")
  )
  private val batchingDurationMetric = metrics.registry.timer(
    MetricRegistry.name(metrics.daml.parallelIndexer.batching.executor, "duration")
  )
  val (failure, minimumUpdateRateFailureInfo): (Boolean, String) =
    config.minUpdateRate match {
      case Some(requiredMinUpdateRate) if requiredMinUpdateRate > updateRate =>
        (
          true,
          s"[failure][UpdateRate] Minimum number of updates per second: required: $requiredMinUpdateRate, metered: $updateRate",
        )
      case _ => (false, "")
    }

  val banner =
    s"""
       |--------------------------------------------------------------------------------
       |Indexer benchmark results
       |--------------------------------------------------------------------------------
       |
       |Input:
       |  source:   ${config.updateSource}
       |  count:    ${config.updateCount}
       |  required updates/sec: ${config.minUpdateRate.getOrElse("-")}
       |  jdbcUrl:  ${config.dataSource.jdbcUrl}
       |
       |Indexer parameters:
       |  maxInputBufferSize:        ${config.indexerConfig.maxInputBufferSize}
       |  inputMappingParallelism:   ${config.indexerConfig.inputMappingParallelism}
       |  ingestionParallelism:      ${config.indexerConfig.ingestionParallelism}
       |  submissionBatchSize:       ${config.indexerConfig.submissionBatchSize}
       |  full indexer config:       ${config.indexerConfig}
       |
       |Result:
       |  duration:    $duration
       |  updates:     $updates
       |  updates/sec: $updateRate
       |  $minimumUpdateRateFailureInfo
       |
       |Other metrics:
       |  inputMapping.batchSize:     ${histogramToString(
        metrics.daml.parallelIndexer.inputMapping.batchSize.getSnapshot
      )}
       |  inputMapping.duration:      ${histogramToString(
        inputMappingDurationMetric.getSnapshot
      )}
       |  inputMapping.duration.rate: ${inputMappingDurationMetric.getMeanRate}
       |  batching.duration:      ${histogramToString(batchingDurationMetric.getSnapshot)}
       |  batching.duration.rate: ${batchingDurationMetric.getMeanRate}
       |  seqMapping.duration: ${histogramToString(
        metrics.daml.parallelIndexer.seqMapping.duration.getSnapshot
      )}|
       |  seqMapping.duration.rate: ${metrics.daml.parallelIndexer.seqMapping.duration.getMeanRate}|
       |  ingestion.duration:         ${histogramToString(
        metrics.daml.parallelIndexer.ingestion.executionTimer.getSnapshot
      )}
       |  ingestion.duration.rate:    ${metrics.daml.parallelIndexer.ingestion.executionTimer.getMeanRate}
       |  tailIngestion.duration:         ${histogramToString(
        metrics.daml.parallelIndexer.tailIngestion.executionTimer.getSnapshot
      )}
       |  tailIngestion.duration.rate:    ${metrics.daml.parallelIndexer.tailIngestion.executionTimer.getMeanRate}
       |
       |Notes:
       |  The above numbers include all ingested updates, including package uploads.
       |  Inspect the metrics using a metrics reporter to better investigate how
       |  the indexer performs.
       |
       |--------------------------------------------------------------------------------
       |""".stripMargin

  private[this] def histogramToString(data: Snapshot): String = {
    s"[min: ${data.getMin}, median: ${data.getMedian}, max: ${data.getMax}]"
  }
}
