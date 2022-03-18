// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import scala.jdk.CollectionConverters._
import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, MetricRegistry, Snapshot, Timer}
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder
import io.prometheus.client.dropwizard.DropwizardExports
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try}

final class ExtendedDropwizardExports(
    metricRegistry: MetricRegistry,
    extractLabels: ExtendedDropwizardExports.LabelsExtractor =
      ExtendedDropwizardExports.NoOpLabelsExtractor,
) extends DropwizardExports(metricRegistry) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sampleBuilder: DefaultSampleBuilder = new DefaultSampleBuilder()

  override def collect(): util.List[MetricFamilySamples] =
    Try {
      allSamples()
        .foldLeft(Map.empty[String, MetricFamilySamples]) {
          (accumulated: Map[String, MetricFamilySamples], newSamples: MetricFamilySamples) =>
            val mergedSamples: MetricFamilySamples = accumulated.get(newSamples.name) match {
              case Some(currentSamples) =>
                mergeSamples(currentSamples, newSamples)
              case None =>
                newSamples
            }
            accumulated + (newSamples.name -> mergedSamples)
        }
        .values
        .toList
        .asJava
    } match {
      case Success(res) =>
        res
      case Failure(ex) =>
        logger.error(s"Metric collection failed", ex)
        throw ex
    }

  private def allSamples(): List[MetricFamilySamples] =
    List(
      metricRegistry
        .getTimers()
        .asScala
        .map { case (name, timer) =>
          val (baseName, labels) = extractLabels(name)
          fromTimer(timer, baseName, labels)
        },
      metricRegistry
        .getHistograms()
        .asScala
        .map { case (name, histogram) =>
          val (baseName, labels) = extractLabels(name)
          fromHistogram(histogram, baseName, labels)
        },
      metricRegistry
        .getCounters()
        .asScala
        .map { case (name, counter) =>
          val (baseName, labels) = extractLabels(name)
          fromCounter(counter, baseName, labels)
        },
      metricRegistry
        .getGauges()
        .asScala
        .flatMap { case (name, gauge) =>
          val (baseName, labels) = extractLabels(name)
          fromGauge(gauge, baseName, labels)
        },
      metricRegistry
        .getMeters()
        .asScala
        .map { case (name, meter) =>
          val (baseName, labels) = extractLabels(name)
          fromMeter(meter, baseName, labels)
        },
    ).flatten

  private def helpMessage(metricName: String) =
    s"Generated from Dropwizard metric import (metric=$metricName) "

  private def mergeSamples(
      samplesA: MetricFamilySamples,
      samplesB: MetricFamilySamples,
  ): MetricFamilySamples = {
    val allSamples = samplesA.samples.asScala ++ samplesB.samples.asScala
    new MetricFamilySamples(
      samplesA.name,
      samplesA.`type`,
      samplesA.help,
      allSamples.asJava,
    )
  }

  private def fromHistogram(
      histogram: Histogram,
      name: String,
      labels: Map[String, String],
  ): MetricFamilySamples =
    fromSnapshotAndCountWithLabels(
      dropwizardName = name,
      snapshot = histogram.getSnapshot,
      count = histogram.getCount,
      factor = 1.0,
      labels = labels,
    )

  private def fromTimer(
      timer: Timer,
      name: String,
      labels: Map[String, String],
  ): MetricFamilySamples =
    fromSnapshotAndCountWithLabels(
      name,
      timer.getSnapshot,
      timer.getCount,
      1.0 / TimeUnit.SECONDS.toNanos(1L),
      labels,
    )

  private def fromCounter(
      counter: Counter,
      name: String,
      labels: Map[String, String],
  ): MetricFamilySamples = {
    val sample = sampleBuilder.createSample(
      name,
      "",
      labels.keys.toList.asJava,
      labels.values.toList.asJava,
      counter.getCount.toDouble,
    )
    new MetricFamilySamples(
      sample.name,
      Collector.Type.GAUGE,
      helpMessage(name),
      List(sample).asJava,
    )
  }

  private def fromGauge(
      gauge: Gauge[_],
      name: String,
      labels: Map[String, String],
  ): Option[MetricFamilySamples] = {
    val value: Option[Double] = gauge.getValue match {
      case x: Double =>
        Some(x)
      case x: Int =>
        Some(x.toDouble)
      case x: Long =>
        Some(x.toDouble)
      case x: Boolean =>
        Some(if (x) 1.0 else 0.0)
      case _ =>
        None
    }
    value.map { v =>
      val sample = sampleBuilder.createSample(
        name,
        "",
        labels.keys.toList.asJava,
        labels.values.toList.asJava,
        v,
      )
      new MetricFamilySamples(
        sample.name,
        Collector.Type.GAUGE,
        helpMessage(name),
        List(sample).asJava,
      )
    }
  }

  private def fromMeter(
      meter: Meter,
      name: String,
      labels: Map[String, String],
  ) = {
    val sample = sampleBuilder.createSample(
      name,
      "_total",
      labels.keys.toList.asJava,
      labels.values.toList.asJava,
      meter.getCount.toDouble,
    )
    new MetricFamilySamples(
      sample.name,
      Collector.Type.COUNTER,
      helpMessage(name),
      List(sample).asJava,
    )
  }

  private def fromSnapshotAndCountWithLabels(
      dropwizardName: String,
      snapshot: Snapshot,
      count: Long,
      factor: Double,
      labels: Map[String, String],
  ): MetricFamilySamples = {
    val labelNames = labels.keys.toList
    val labelNamesJava = labelNames.asJava
    val labelValues = labels.values.toList
    val labelValuesJava = labelValues.asJava
    val samples = List(
      sampleBuilder.createSample(
        dropwizardName,
        "",
        (List("quantile") ++ labelNames).asJava,
        (List("0.5") ++ labelValues).asJava,
        snapshot.getMedian * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "",
        (List("quantile") ++ labelNames).asJava,
        (List("0.75") ++ labelValues).asJava,
        snapshot.get75thPercentile() * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "",
        (List("quantile") ++ labelNames).asJava,
        (List("0.95") ++ labelValues).asJava,
        snapshot.get95thPercentile() * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "",
        (List("quantile") ++ labelNames).asJava,
        (List("0.98") ++ labelValues).asJava,
        snapshot.get98thPercentile() * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "",
        (List("quantile") ++ labelNames).asJava,
        (List("0.99") ++ labelValues).asJava,
        snapshot.get99thPercentile() * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "",
        (List("quantile") ++ labelNames).asJava,
        (List("0.999") ++ labelValues).asJava,
        snapshot.get999thPercentile() * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "_count",
        labelNamesJava,
        labelValuesJava,
        count.toDouble,
      ),
      sampleBuilder
        .createSample(
          dropwizardName,
          "_min",
          labelNamesJava,
          labelValuesJava,
          snapshot.getMin * factor,
        ),
      sampleBuilder.createSample(
        dropwizardName,
        "_mean",
        labelNamesJava,
        labelValuesJava,
        snapshot.getMean * factor,
      ),
      sampleBuilder.createSample(
        dropwizardName,
        "_max",
        labelNamesJava,
        labelValuesJava,
        snapshot.getMax * factor,
      ),
    )

    new MetricFamilySamples(
      samples.head.name,
      Collector.Type.SUMMARY,
      helpMessage(dropwizardName),
      samples.asJava,
    )
  }
}

object ExtendedDropwizardExports {
  type LabelsExtractor = String => (String, Map[String, String])
  val NoOpLabelsExtractor: LabelsExtractor = labelName => (labelName, Map.empty)
}
