// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.metrics.{HealthMetrics as DMHealth, HistogramDefinition}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.domain.metrics.{MediatorMetrics, SequencerMetrics}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.CantonOpenTelemetryMetricsFactory
import com.digitalasset.canton.metrics.MetricsConfig.MetricsFilterConfig
import com.digitalasset.canton.metrics.MetricsReporterConfig.{Csv, Prometheus}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.{DiscardOps, DomainAlias}
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder

import java.io.File
import scala.collection.concurrent.TrieMap

final case class MetricsConfig(
    reporters: Seq[MetricsReporterConfig] = Seq.empty,
    reportJvmMetrics: Boolean = false,
    histograms: Seq[HistogramDefinition] = Seq.empty,
)

object MetricsConfig {

  final case class MetricsFilterConfig(
      startsWith: String = "",
      contains: String = "",
      endsWith: String = "",
  ) {
    def matches(name: String): Boolean =
      name.startsWith(startsWith) && name.contains(contains) && name.endsWith(endsWith)
  }
}

sealed trait MetricsReporterConfig {
  def filters: Seq[MetricsFilterConfig]

}

object MetricsReporterConfig {

  final case class Prometheus(address: String = "localhost", port: Port = Port.tryCreate(9464))
      extends MetricsReporterConfig {
    // TODO(#16647): ensure prometheus metrics are filtered
    override def filters: Seq[MetricsFilterConfig] = Seq.empty
  }

  /** CSV metrics reporter configuration
    *
    * This reporter will write the given metrics into respective csv files. Please note that you should use
    * filters as otherwise, you'll get many files
    *
    * @param directory where to write the csv files to
    * @param interval how often to write the csv files
    * @param contextKeys which context keys to include in the name. defaults to node names
    * @param filters which metrics to include
    */
  final case class Csv(
      directory: File,
      interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
      contextKeys: Set[String] = Set("node", "domain"),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
  ) extends MetricsReporterConfig

}
final case class MetricsRegistry(
    reportJVMMetrics: Boolean,
    meter: Meter,
    factoryType: MetricsFactoryType,
) extends AutoCloseable
    with CantonLabeledMetricsFactory.Generator {

  private val participants = TrieMap[String, ParticipantMetrics]()
  private val sequencers = TrieMap[String, SequencerMetrics]()
  private val mediators = TrieMap[String, MediatorMetrics]()

  // add default, system wide metrics to the metrics reporter
  if (reportJVMMetrics) {
    // TODO(#16647): re-enable jvm metrics
    // registry.registerAll(new JvmMetricSet) // register Daml repo JvmMetricSet
    // JvmMetricSet.registerObservers() // requires OpenTelemetry to have the global lib setup
  }

  def forParticipant(name: String): ParticipantMetrics = {
    participants.getOrElseUpdate(
      name, {
        val participantMetricsContext =
          MetricsContext("node" -> name, "component" -> "participant")
        new ParticipantMetrics(
          MetricsRegistry.prefix,
          create(
            participantMetricsContext
          ),
        )
      },
    )
  }

  def forSequencer(name: String): SequencerMetrics = {
    sequencers.getOrElseUpdate(
      name, {
        val sequencerMetricsContext =
          MetricsContext("node" -> name, "component" -> "sequencer")
        val labeledMetricsFactory = create(
          sequencerMetricsContext
        )
        new SequencerMetrics(
          MetricsRegistry.prefix,
          labeledMetricsFactory,
          new DamlGrpcServerMetrics(labeledMetricsFactory, "sequencer"),
          new DMHealth(labeledMetricsFactory),
        )
      },
    )
  }

  def forMediator(name: String): MediatorMetrics = {
    mediators.getOrElseUpdate(
      name, {
        val mediatorMetricsContext = MetricsContext("node" -> name, "component" -> "mediator")
        val labeledMetricsFactory =
          create(mediatorMetricsContext)
        new MediatorMetrics(
          MetricsRegistry.prefix,
          labeledMetricsFactory,
          new DamlGrpcServerMetrics(labeledMetricsFactory, "mediator"),
          new DMHealth(labeledMetricsFactory),
        )
      },
    )
  }

  override def create(
      extraContext: MetricsContext
  ): CantonLabeledMetricsFactory = {
    factoryType match {
      case MetricsFactoryType.InMemory(provider) =>
        provider(extraContext)
      case MetricsFactoryType.External =>
        new CantonOpenTelemetryMetricsFactory(
          meter,
          globalMetricsContext = extraContext,
        )
    }
  }

  /** returns the documented metrics by possibly creating fake participants / sequencers / mediators */
  def metricsDoc(): (Seq[MetricDoc.Item], Seq[MetricDoc.Item], Seq[MetricDoc.Item]) = {
    def sorted(lst: Seq[MetricDoc.Item]): Seq[MetricDoc.Item] =
      lst
        .groupBy(_.name)
        .flatMap(_._2.headOption.toList)
        .toSeq
        .sortBy(_.name)

    val participantMetrics: ParticipantMetrics =
      participants.headOption.map(_._2).getOrElse(forParticipant("dummyParticipant"))
    val participantItems = MetricDoc.getItems(participantMetrics)
    val clientMetrics =
      MetricDoc.getItems(participantMetrics.domainMetrics(DomainAlias.tryCreate("<domain>")))
    val sequencerMetrics = MetricDoc.getItems(
      sequencers.headOption
        .map { case (_, sequencerMetrics) => sequencerMetrics }
        .getOrElse(forSequencer("dummySequencer"))
    )
    val mediatorMetrics = MetricDoc.getItems(
      mediators.headOption
        .map { case (_, mediatorMetrics) => mediatorMetrics }
        .getOrElse(forSequencer("dummyMediator"))
    )

    // the fake instances are fine here as we do this anyway only when we build and export the docs
    (sorted(participantItems ++ clientMetrics), sorted(sequencerMetrics), sorted(mediatorMetrics))
  }

  override def close(): Unit = ()

}

object MetricsRegistry extends LazyLogging {

  val prefix: MetricName = MetricName.Daml

  def registerReporters(
      config: MetricsConfig,
      loggerFactory: NamedLoggerFactory,
  )(
      sdkMeterProviderBuilder: SdkMeterProviderBuilder
  ): SdkMeterProviderBuilder = {
    if (config.reporters.isEmpty) {
      logger.info("No metrics reporters configured. Not starting metrics collection.")
    }
    config.reporters.foreach {
      case Prometheus(hostname, port) =>
        logger.info(s"Exposing metrics for Prometheus on port $hostname:$port")
        val prometheusServer = PrometheusHttpServer
          .builder()
          .setHost(hostname)
          .setPort(port.unwrap)
          .newMetricReaderFactory()
        sdkMeterProviderBuilder.registerMetricReader(prometheusServer).discard
      case config: Csv =>
        sdkMeterProviderBuilder
          .registerMetricReader(
            new CsvReporter(config, loggerFactory)
          )
          .discard
    }
    sdkMeterProviderBuilder
  }

}
