// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.indexerbenchmark

import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Timer}
import com.daml.metrics.api.noop.{NoOpCounter, NoOpTimer}
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{
  InMemoryCounter,
  InMemoryHistogram,
  InMemoryTimer,
}
import com.daml.metrics.api.testing.MetricValues
import com.digitalasset.canton.metrics.LedgerApiServerMetrics

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationDouble

class IndexerBenchmarkResult(
    config: Config,
    metrics: LedgerApiServerMetrics,
    startTimeInNano: Long,
    stopTimeInNano: Long,
) extends MetricValues {

  private val duration: Double =
    (stopTimeInNano - startTimeInNano).toDouble.nanos.toUnit(TimeUnit.SECONDS)
  private val updates: Long = counterState(metrics.parallelIndexer.updates)
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
        metrics.parallelIndexer.inputMapping.batchSize
      )}
       |  seqMapping.duration: ${timerToString(
        metrics.parallelIndexer.seqMapping.duration
      )}|
       |  seqMapping.duration.rate: ${timerMeanRate(
        metrics.parallelIndexer.seqMapping.duration
      )}|
       |  ingestion.duration:         ${timerToString(
        metrics.parallelIndexer.ingestion.executionTimer
      )}
       |  ingestion.duration.rate:    ${timerMeanRate(
        metrics.parallelIndexer.ingestion.executionTimer
      )}
       |  tailIngestion.duration:         ${timerToString(
        metrics.parallelIndexer.tailIngestion.executionTimer
      )}
       |  tailIngestion.duration.rate:    ${timerMeanRate(
        metrics.parallelIndexer.tailIngestion.executionTimer
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

      case _: InMemoryHistogram =>
        recordedHistogramValuesToString(histogram.values)

      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def timerToString(timer: Timer): String = {
    timer match {
      case NoOpTimer(_) => ""
      case _: InMemoryTimer =>
        recordedHistogramValuesToString(timer.values)

      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def timerMeanRate(timer: Timer): Double = {
    timer match {
      case NoOpTimer(_) => 0
      case timer: InMemoryTimer =>
        timer.data.values.size.toDouble / duration
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def counterState(counter: Counter): Long = {
    counter match {
      case NoOpCounter(_) => 0
      case InMemoryCounter(_, _) => counter.value

      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private def recordedHistogramValuesToString(data: Seq[Long]) = {
    s"[min: ${data.foldLeft(0L)(_.min(_))}, median: ${median(data)}, max: ${data.foldLeft(0L)(_.max(_))}"
  }

  private def median(data: Seq[Long]) = {
    val sorted = data.sorted
    if (sorted.size % 2 == 0) {
      (sorted(sorted.size / 2 - 1) + (sorted.size / 2)) / 2
    } else {
      sorted(sorted.size / 2)
    }
  }
}
