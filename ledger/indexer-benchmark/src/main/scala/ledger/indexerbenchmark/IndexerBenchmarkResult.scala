// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.indexerbenchmark

import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricHandle.{Histogram, Timer}
import com.daml.metrics.api.noop.NoOpTimer
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{InMemoryHistogram, InMemoryTimer}
import com.daml.metrics.api.testing.MetricValues

class IndexerBenchmarkResult(config: Config, metrics: Metrics, startTime: Long, stopTime: Long)
    extends MetricValues {

  private val duration: Double = (stopTime - startTime).toDouble / 1000000000.0
  private val updates: Long = metrics.daml.parallelIndexer.updates.value
  private val updateRate: Double = updates / duration

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
      case histogram: InMemoryHistogram =>
        val data = histogram.values.flatMap(_._2).toSeq
        dropwizardSnapshotToString(data.min, data.max, data(data.size / 2).toDouble)
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def timerToString(timer: Timer): String = {
    timer match {
      case timer: InMemoryTimer =>
        val values = timer.values
        dropwizardSnapshotToString(values.min, values.max, values(values.size / 2).toDouble)
      case NoOpTimer(_) => ""
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def timerMeanRate(timer: Timer): Double = {
    timer match {
      case timer: InMemoryTimer =>
        timer.values.sum.toDouble / timer.values.size
      case NoOpTimer(_) => 0
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }
  private def dropwizardSnapshotToString(min: Long, max: Long, median: Double) = {
    s"[min: $min, median: $median, max: $max"
  }
}
