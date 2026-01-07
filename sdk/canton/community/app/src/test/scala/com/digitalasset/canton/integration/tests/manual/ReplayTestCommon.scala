// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.File
import com.daml.metrics.HistogramDefinition
import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.config
import com.digitalasset.canton.config.MonitoringConfig
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.integration.{ConfigTransform, ConfigTransforms}
import com.digitalasset.canton.metrics.MetricsConfig
import com.digitalasset.canton.metrics.MetricsConfig.JvmMetrics
import com.digitalasset.canton.metrics.MetricsReporterConfig.Prometheus
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import monocle.macros.syntax.lens.*

import java.nio.file.Path
import scala.concurrent.duration.DurationInt

object ReplayTestCommon {
  val ReplayTestSystemPropertyPrefix = "replay-tests"

  // Should be equal or greater than the number of sequencers. Otherwise, not all sequencers would get submissions.
  val numberOfParticipants: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.num-participants"))
      .map(_.toInt)
      .getOrElse(2)
  val numberOfSequencers: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.num-sequencers"))
      .map(_.toInt)
      .getOrElse(1)
  val numberOfMediators: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.num-mediators"))
      .map(_.toInt)
      .getOrElse(1)

  // Whether to use separate single-node mediator groups.
  // When set to `false` (default), uses a single mediator group with `f+1` threshold, where `f` is the tolerable number
  //  of faulty nodes. This setup likely has worse performance, because BFT mediators trigger message aggregation.
  //  Sequencers aggregate verdicts from mediators, and once they reach the threshold, they forward them to participants.
  //  That's the setup used by Canton Network.
  val useSeparateMediatorGroups: Boolean =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.use-separate-mediator-groups"))
      .exists(_.toBoolean)

  val numberOfDbConnectionsPerNode: PositiveInt =
    PositiveInt.tryCreate(
      Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.num-db-connections-per-node"))
        .map(_.toInt)
        .getOrElse(5)
    )

  private def prometheusMetricsConfig(httpServerPort: Port): MetricsConfig =
    MetricsConfig(
      reporters = Seq(
        Prometheus(
          address = "0.0.0.0",
          port = httpServerPort,
        )
      ),
      // Raised to allow for per domain member labels (see #16410)
      cardinality = PositiveInt.tryCreate(20000),
      histograms = Seq(
        HistogramDefinition(
          name = "*",
          aggregation = HistogramDefinition.Exponential(
            maxBuckets = 160,
            maxScale = 20,
          ),
        )
      ),
      qualifiers = Seq[MetricQualification](
        MetricQualification.Errors,
        MetricQualification.Latency,
        MetricQualification.Saturation,
        MetricQualification.Traffic,
        MetricQualification.Debug,
      ),
      jvmMetrics = Some(JvmMetrics(enabled = true)),
    )

  def metricsConfig(
      prometheusHttpServerPort: Port,
      prefix: String = ReplayTestSystemPropertyPrefix,
  ): MetricsConfig = {
    val enabled =
      Option(System.getProperty(s"$prefix.enable-prometheus-metrics"))
        .exists(_.toBoolean)

    if (enabled) prometheusMetricsConfig(prometheusHttpServerPort)
    else MetricsConfig() // disabled by default
  }

  def configTransforms(
      prometheusHttpServerPort: Port = Port.tryCreate(19090),
      prefix: String = ReplayTestSystemPropertyPrefix,
  ): Seq[ConfigTransform] =
    ConfigTransforms.allDefaultsButGloballyUniquePorts ++ Seq(
      ConfigTransforms.disableAdditionalConsistencyChecks,
      ConfigTransforms.setDelayLoggingThreshold(
        config.NonNegativeFiniteDuration.tryFromDuration(30.seconds)
      ),
      _.focus(_.monitoring).replace(
        MonitoringConfig(
          metrics = metricsConfig(prometheusHttpServerPort, prefix),
          tracing = TracingConfig(propagation = Propagation.Enabled),
        )
      ),
    )

  def dumpDirectory(baseDirectory: File): File = baseDirectory / "dumps"
  def warmupDirectory(baseDirectory: File): File = baseDirectory / "warmup-events"
  def testDirectory(baseDirectory: File): File = baseDirectory / "test-events"

  def dumpPathOfNode(nodeName: String, baseDirectory: File): Path =
    (dumpDirectory(baseDirectory) / s"$nodeName.tar").path
}
