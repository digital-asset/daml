// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance

import com.codahale.metrics.MetricRegistry
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftBenchmarkTool.NanosInMillis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.performance.BftMetrics.{
  MetricName,
  NamedMetric,
  failedWriteMeters,
  pendingReads,
  readMeters,
  roundTripNanosHistogram,
  startedWriteMeters,
  successfulWriteMeters,
  writeNanosHistograms,
}
import com.digitalasset.canton.tracing.TraceContext
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.typesafe.config.ConfigRenderOptions
import pureconfig.ConfigWriter
import pureconfig.generic.auto.*

import java.io.StringWriter
import java.util.concurrent.{Executors, Future, TimeUnit}
import scala.collection.SeqMap
import scala.collection.immutable.ListMap
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class BftBenchmarkTool(
    bftBindingFactory: BftBindingFactory,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private implicit val traceContext: TraceContext = TraceContext.empty

  def run(config: BftBenchmarkConfig): SeqMap[MetricName, AnyVal] = {

    val renderOptions: ConfigRenderOptions = ConfigRenderOptions
      .defaults()
      .setOriginComments(false)
      .setComments(false)
      .setJson(true)
      .setFormatted(false)

    val nodeIndices = config.nodes.indices

    val readNodeIndices =
      config.nodes.filter(_.isInstanceOf[BftBenchmarkConfig.ReadNode[?]]).indices

    logger.info(
      "Starting BFT benchmark, the configuration follows (JSON)"
    )

    logger.info(
      ConfigWriter[BftBenchmarkConfig]
        .to(config)
        .render(renderOptions.setFormatted(false))
    )

    val metrics = new MetricRegistry()

    val bftBenchmark = new BftBenchmark(config, bftBindingFactory, metrics)

    val bftBenchmarkDoneFuture = bftBenchmark.run()
    val reportingScheduler = Executors.newScheduledThreadPool(1)
    val scheduledReport = config.reportingInterval.map { reportingInterval =>
      reportingScheduler.scheduleAtFixedRate(
        { () => report(nodeIndices, readNodeIndices, metrics); () }: Runnable,
        reportingInterval.toNanos,
        reportingInterval.toNanos,
        TimeUnit.NANOSECONDS,
      )
    }
    awaitBftBenchmarkDoneFuture(config, bftBenchmarkDoneFuture)
    scheduledReport.foreach(_.cancel(true))
    reportingScheduler.shutdown()

    // Always include a final report
    logger.info("Completed, final stats will follow")
    report(nodeIndices, readNodeIndices, metrics)
  }

  private def awaitBftBenchmarkDoneFuture(
      config: BftBenchmarkConfig,
      bftBenchmarkDoneFuture: Future[Unit],
  ): Unit =
    bftBenchmarkDoneFuture.get(config.runDuration.toNanos + 2.minutes.toNanos, TimeUnit.NANOSECONDS)

  private def report(
      nodeIndices: Range,
      readNodeIndices: Range,
      metrics: MetricRegistry,
  ): SeqMap[MetricName, AnyVal] = {
    val meterReport =
      ("pending.reads.count" -> pendingReads) +:
        Seq(
          startedWriteMeters(metrics, nodeIndices),
          successfulWriteMeters(metrics, nodeIndices),
          failedWriteMeters(metrics, nodeIndices),
          readMeters(metrics, readNodeIndices),
        ).flatten.flatMap { case NamedMetric(name, meter) =>
          Seq(
            s"$name.rate.mean" -> meter.getMeanRate,
            s"$name.count" -> meter.getCount,
          )
        }

    val histogramReport =
      List.newBuilder
        .addAll(
          writeNanosHistograms(
            metrics,
            nodeIndices,
          )
        )
        .addOne(
          roundTripNanosHistogram(metrics)
        )
        .result()
        .flatMap { case NamedMetric(name, histogram) =>
          val snapshot = histogram.getSnapshot
          Seq(
            s"$name.ms.mean" -> snapshot.getMean,
            s"$name.ms.median" -> snapshot.getMedian,
            s"$name.ms.min" -> snapshot.getMin.toDouble,
            s"$name.ms.95%" -> snapshot.get95thPercentile,
            s"$name.ms.99%" -> snapshot.get99thPercentile,
            s"$name.ms.max" -> snapshot.getMax.toDouble,
          ).map { case (name, nanos) =>
            name -> nanos / NanosInMillis
          }
        }

    // This is compatible with Jackson and also preserves the intended metric names order (i.e., insertion order).
    val unifiedReport: SeqMap[MetricName, AnyVal] =
      ListMap.newBuilder.addAll((meterReport ++ histogramReport).sortBy(_._1)).result()

    val reportString = toJson(unifiedReport.asJava)
    logger.info(reportString)
    println(reportString)
    unifiedReport
  }

  private def toJson(request: Any): String = {
    val stringWriter = new StringWriter
    val objectMapper = {
      val mapper = new ObjectMapper()
      mapper.enable(SerializationFeature.INDENT_OUTPUT)
    }
    objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
    objectMapper.writeValue(stringWriter, request)
    stringWriter.toString
  }
}

object BftBenchmarkTool {

  private val NanosInMillis = 1_000_000
}
