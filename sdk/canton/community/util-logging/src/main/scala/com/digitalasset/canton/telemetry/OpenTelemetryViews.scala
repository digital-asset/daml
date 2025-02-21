// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.metrics.HistogramDefinition
import com.daml.metrics.HistogramDefinition.AggregationType
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer
import com.daml.metrics.api.{HistogramInventory, MetricsInfoFilter}
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.sdk.metrics.*

import scala.jdk.CollectionConverters.SeqHasAsJava

object OpenTelemetryViews extends LazyLogging {

  /** Add views to providers
    *
    * In open telemetry, you have to define the histogram views separately from the metric itself.
    * Even worse, you need to define it before you define the metrics. If you define two views that
    * match to the same metrics, you end up with ugly warning messages and errors:
    * https://opentelemetry.io/docs/specs/otel/metrics/sdk/#measurement-processing
    *
    * The solution to this is to statically define all the metric names in advance separately,
    * create appropriate views and then, on each histogram definition check that an appropriate
    * static definition exists.
    */
  def addViewsToProvider(
      builder: SdkMeterProviderBuilder,
      testingSupportAdhocMetrics: Boolean,
      histogramInventory: HistogramInventory,
      histogramFilter: MetricsInfoFilter,
      histogramConfigs: Seq[HistogramDefinition],
  ): SdkMeterProviderBuilder =
    histogramConfigs match {
      // Add global support for exponential histograms when the only custom histogram definition is an exponential config with a catch all
      // this is done to ensure that all histograms use the same view, even histograms that aren't defined by us and thus aren't part of the inventory
      case Seq(HistogramDefinition("*", HistogramDefinition.Exponential(buckets, scale), _)) =>
        logger.info("Enabling exponential histograms")
        builder.registerView(
          histogramSelectorByName("*"),
          View
            .builder()
            .setAggregation(Aggregation.base2ExponentialBucketHistogram(buckets, scale))
            .build(),
        )
      case _ =>
        val timeHistograms = (
          s"*${OpenTelemetryTimer.TimerUnitAndSuffix}",
          HistogramDefinition.Buckets(
            Seq(
              0.01d, 0.025d, 0.050d, 0.075d, 0.1d, 0.15d, 0.2d, 0.25d, 0.35d, 0.5d, 0.75d, 1d, 2.5d,
              5d, 10d,
            )
          ),
        )
        val byteHistograms = (
          s"*${Histogram.Bytes}",
          HistogramDefinition.Buckets(
            Seq(
              kilobytes(10),
              kilobytes(50),
              kilobytes(100),
              kilobytes(500),
              megabytes(1),
              megabytes(5),
              megabytes(10),
              megabytes(50),
            )
          ),
        )
        if (testingSupportAdhocMetrics) {
          // Register our default timers and byte histograms
          Seq(timeHistograms, byteHistograms).foldLeft(builder) {
            case (builder, (name, aggregation)) =>
              builder.registerView(
                histogramSelectorByName(name),
                explicitHistogramBucketsView(name, aggregation, None),
              )
          }
        } else {
          // due to https://opentelemetry.io/docs/specs/otel/metrics/sdk/#measurement-processing
          // we need to have exactly one view definition per instrument name and not multiple
          // otherwise you get lots of error messages dumped into the logs
          val configs = histogramConfigs ++ Seq(timeHistograms, byteHistograms).map {
            case (name, buckets) => HistogramDefinition(name, buckets)
          }
          // for each known histogram, register a view
          histogramInventory
            .registered()
            .filter(item => histogramFilter.includeMetric(item.info))
            .flatMap { item =>
              // find the histogram configs that matches the name
              // there might be multiple views for the same instrument
              val instrumentConfigs = configs
                .filter(_.matches(item.name))
                .groupBy(_.viewName.getOrElse(item.name.toString))
                .toSeq
                .flatMap { case (_, allMatchingDefinitions) =>
                  allMatchingDefinitions.headOption.map(first => (item, first))
                }
              if (instrumentConfigs.nonEmpty) instrumentConfigs
              else
                Seq(
                  (
                    item,
                    HistogramDefinition(item.name, HistogramDefinition.Buckets(Seq.empty)),
                  )
                )
            }
            .foldLeft(builder) { case (builder, (item, definition)) =>
              logger.debug(s"Registering view $definition for item $item")
              builder.registerView(
                histogramSelectorByName(item.name),
                explicitHistogramBucketsView(
                  item.summary,
                  definition.aggregation,
                  definition.viewName,
                ),
              )
            }
        }
    }

  private def histogramSelectorByName(stringWithWildcards: String) = InstrumentSelector
    .builder()
    .setType(InstrumentType.HISTOGRAM)
    .setName(stringWithWildcards)
    .build()

  private def explicitHistogramBucketsView(
      summary: String,
      aggregationType: AggregationType,
      viewName: Option[String],
  ) = {
    val aggregation = aggregationType match {
      case HistogramDefinition.Buckets(buckets) =>
        if (buckets.nonEmpty)
          Aggregation.explicitBucketHistogram(
            buckets.map(Double.box).asJava
          )
        else Aggregation.explicitBucketHistogram()
      case HistogramDefinition.Exponential(maxBuckets, maxScale) =>
        Aggregation.base2ExponentialBucketHistogram(maxBuckets, maxScale)
    }
    val tmp = View
      .builder()
      .setAggregation(aggregation)
      .setDescription(summary)
    viewName.foreach(tmp.setName)
    tmp.build()
  }

  private def kilobytes(value: Int): Double = value * 1024d

  private def megabytes(value: Int): Double = value * 1024d * 1024d

}
