// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import com.codahale.metrics.Snapshot
import com.daml.metrics.MetricHandle.{Histogram, Timer}
import com.daml.metrics.{MetricHandle, Metrics}

class IndexerBenchmarkResult(config: Config, metrics: Metrics, startTime: Long, stopTime: Long) {

  private val duration: Double = (stopTime - startTime).toDouble / 1000000000.0
  private val updates: Long = metrics.daml.parallelIndexer.updates.getCount
  private val updateRate: Double = updates / duration
  private val inputMappingDurationMetric = metrics.timer(
    metrics.daml.parallelIndexer.inputMapping.executor :+ "duration"
  )
  private val batchingDurationMetric = metrics.timer(
    metrics.daml.parallelIndexer.batching.executor :+ "duration"
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
        metrics.daml.parallelIndexer.inputMapping.batchSize
      )}
       |  inputMapping.duration:      ${timerToString(
        inputMappingDurationMetric
      )}
       |  inputMapping.duration.rate: ${timerMeanRate(inputMappingDurationMetric)}
       |  batching.duration:      ${timerToString(batchingDurationMetric)}
       |  batching.duration.rate: ${timerMeanRate(batchingDurationMetric)}
       |  seqMapping.duration: ${timerToString(
        metrics.daml.parallelIndexer.seqMapping.duration
      )}|
       |  seqMapping.duration.rate: ${timerMeanRate(
        metrics.daml.parallelIndexer.seqMapping.duration
      )}|
       |  ingestion.duration:         ${timerToString(
        metrics.daml.parallelIndexer.ingestion.executionTimer
      )}
       |  ingestion.duration.rate:    ${timerMeanRate(
        metrics.daml.parallelIndexer.ingestion.executionTimer
      )}
       |  tailIngestion.duration:         ${timerToString(
        metrics.daml.parallelIndexer.tailIngestion.executionTimer
      )}
       |  tailIngestion.duration.rate:    ${timerMeanRate(
        metrics.daml.parallelIndexer.tailIngestion.executionTimer
      )}
       |
       |Notes:
       |  The above numbers include all ingested updates, including package uploads.
       |  Inspect the metrics using a metrics reporter to better investigate how
       |  the indexer performs.
       |
       |--------------------------------------------------------------------------------
       |""".stripMargin

  private[this] def histogramToString(histogram: Histogram): String = {
    histogram match {
      case MetricHandle.DropwizardHistogram(_, metric) =>
        val data = metric.getSnapshot
        dropwizardSnapshotToString(data)
    }
  }

  private[this] def timerToString(timer: Timer): String = {
    timer match {
      case MetricHandle.DropwizardTimer(_, metric) =>
        val data = metric.getSnapshot
        dropwizardSnapshotToString(data)
      case MetricHandle.Timer.NoOpTimer(_) => ""
    }
  }

  private[this] def timerMeanRate(timer: Timer): Double = {
    timer match {
      case MetricHandle.DropwizardTimer(_, metric) =>
        metric.getMeanRate
      case MetricHandle.Timer.NoOpTimer(_) => 0
    }
  }
  private def dropwizardSnapshotToString(data: Snapshot) = {
    s"[min: ${data.getMin}, median: ${data.getMedian}, max: ${data.getMax}"
  }
}
