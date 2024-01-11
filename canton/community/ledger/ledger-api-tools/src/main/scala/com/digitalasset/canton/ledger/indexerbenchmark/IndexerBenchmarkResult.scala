// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.indexerbenchmark

import com.codahale.metrics.Snapshot
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Timer}
import com.daml.metrics.api.dropwizard.{DropwizardCounter, DropwizardHistogram, DropwizardTimer}
import com.daml.metrics.api.noop.{NoOpCounter, NoOpTimer}
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{
  InMemoryCounter,
  InMemoryHistogram,
  InMemoryTimer,
}
import com.daml.metrics.api.testing.MetricValues
import com.daml.metrics.api.testing.ProxyMetricsFactory.{ProxyCounter, ProxyHistogram, ProxyTimer}
import com.digitalasset.canton.metrics.Metrics

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationDouble

class IndexerBenchmarkResult(
    config: Config,
    metrics: Metrics,
    startTimeInNano: Long,
    stopTimeInNano: Long,
) extends MetricValues {

  private val duration: Double =
    (stopTimeInNano - startTimeInNano).toDouble.nanos.toUnit(TimeUnit.SECONDS)
  private val updates: Long = counterState(metrics.daml.parallelIndexer.updates)
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
      case DropwizardHistogram(_, metric) =>
        val data = metric.getSnapshot
        dropwizardSnapshotToString(data)
      case _: InMemoryHistogram =>
        recordedHistogramValuesToString(histogram.values)
      case ProxyHistogram(_, targets) =>
        targets
          .collectFirst { case inMemory: InMemoryHistogram =>
            inMemory
          }
          .fold(throw new IllegalArgumentException(s"Histogram $histogram cannot be printed."))(
            histogramToString
          )
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def timerToString(timer: Timer): String = {
    timer match {
      case DropwizardTimer(_, metric) =>
        val data = metric.getSnapshot
        dropwizardSnapshotToString(data)
      case NoOpTimer(_) => ""
      case _: InMemoryTimer =>
        recordedHistogramValuesToString(timer.values)
      case ProxyTimer(_, targets) =>
        targets
          .collectFirst { case inMemory: InMemoryTimer =>
            inMemory
          }
          .fold(throw new IllegalArgumentException(s"Timer $timer cannot be printed."))(
            timerToString
          )
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def timerMeanRate(timer: Timer): Double = {
    timer match {
      case DropwizardTimer(_, metric) =>
        metric.getMeanRate
      case NoOpTimer(_) => 0
      case timer: InMemoryTimer =>
        timer.data.values.size.toDouble / duration
      case ProxyTimer(_, targets) =>
        targets
          .collectFirst { case inMemory: InMemoryTimer =>
            inMemory
          }
          .fold(throw new IllegalArgumentException(s"Timer $timer cannot be printed."))(
            timerMeanRate
          )
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private[this] def counterState(counter: Counter): Long = {
    counter match {
      case DropwizardCounter(_, metric) =>
        metric.getCount
      case NoOpCounter(_) => 0
      case InMemoryCounter(_, _) => counter.value
      case ProxyCounter(_, targets) =>
        targets
          .collectFirst { case inMemory: InMemoryCounter =>
            inMemory
          }
          .fold(throw new IllegalArgumentException(s"Counter $counter cannot be printed."))(
            counterState
          )
      case other => throw new IllegalArgumentException(s"Metric $other not supported")
    }
  }

  private def dropwizardSnapshotToString(data: Snapshot) = {
    s"[min: ${data.getMin}, median: ${data.getMedian}, max: ${data.getMax}"
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
