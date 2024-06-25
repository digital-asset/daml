// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.opentelemetry.{
  OpenTelemetryMetricsFactory,
  QualificationFilteringMetricsFactory,
}
import com.daml.metrics.api.{MetricQualification, MetricsContext, MetricsInfoFilter}
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.metrics.{HealthMetrics, HistogramDefinition, MetricsFilterConfig}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.{MediatorMetrics, SequencerMetrics}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsConfig.JvmMetrics
import com.digitalasset.canton.metrics.MetricsReporterConfig.{Csv, Logging, Prometheus}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.instrumentation.runtimemetrics.java8.*
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder
import io.opentelemetry.sdk.metrics.`export`.{MetricExporter, MetricReader, PeriodicMetricReader}

import java.io.File
import java.util.concurrent.ScheduledExecutorService
import scala.collection.concurrent.TrieMap

/** Configure metric instrumentiation
  *
  * @param reporters which reports should be used to report metric output
  * @param jvmMetrics if true, then JvmMetrics will be reported
  * @param histograms customized histogram definitions
  * @param qualifiers which metric qualifiers to include generally. by default, all except Debug metrics
  *                   are included. The qualifier filtering takes precedence over the individual reporter filters
  */
final case class MetricsConfig(
    reporters: Seq[MetricsReporterConfig] = Seq.empty,
    jvmMetrics: Option[JvmMetrics] = None,
    histograms: Seq[HistogramDefinition] = Seq.empty,
    qualifiers: Seq[MetricQualification] = Seq[MetricQualification](
      MetricQualification.Errors,
      MetricQualification.Latency,
      MetricQualification.Saturation,
      MetricQualification.Traffic,
    ),
) {

  // if empty, no filter, otherwise, the union of all filters
  val globalFilters: Seq[MetricsFilterConfig] = {
    if (reporters.exists(_.filters.isEmpty)) Seq.empty
    else {
      reporters.flatMap(_.filters).distinct
    }
  }

}

object MetricsConfig {

  /** Control and enable jvm metrics */
  final case class JvmMetrics(
      enabled: Boolean = false,
      classes: Boolean = true,
      cpu: Boolean = true,
      memoryPools: Boolean = true,
      threads: Boolean = true,
      gc: Boolean = true,
  )

  object JvmMetrics {
    def setup(config: JvmMetrics, openTelemetry: OpenTelemetry): Unit = {
      if (config.enabled) {
        if (config.classes) Classes.registerObservers(openTelemetry).discard
        if (config.cpu) Cpu.registerObservers(openTelemetry).discard
        if (config.memoryPools) MemoryPools.registerObservers(openTelemetry).discard
        if (config.threads) Threads.registerObservers(openTelemetry).discard
        if (config.gc) GarbageCollector.registerObservers(openTelemetry).discard
      }
    }
  }

}

sealed trait MetricsReporterConfig {
  def filters: Seq[MetricsFilterConfig]

}

object MetricsReporterConfig {

  final case class Prometheus(
      address: String = "localhost",
      port: Port = Port.tryCreate(9464),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
  ) extends MetricsReporterConfig

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

  /** Log metrics reporter configuration
    *
    * This reporter will log the metrics in the given interval
    *
    * @param interval how often to log the metrics
    * @param filters which metrics to include
    */
  final case class Logging(
      interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
      logAsInfo: Boolean = true,
  ) extends MetricsReporterConfig

}
final case class MetricsRegistry(
    meter: Meter,
    factoryType: MetricsFactoryType,
    testingSupportAdhocMetrics: Boolean,
    histograms: CantonHistograms,
    baseFilter: MetricsInfoFilter,
    loggerFactory: NamedLoggerFactory,
) extends AutoCloseable
    with MetricsFactoryProvider
    with NamedLogging {

  private val participants = TrieMap[String, ParticipantMetrics]()
  private val sequencers = TrieMap[String, SequencerMetrics]()
  private val mediators = TrieMap[String, MediatorMetrics]()

  def forParticipant(name: String): ParticipantMetrics = {
    participants.getOrElseUpdate(
      name, {
        val participantMetricsContext =
          MetricsContext("node" -> name, "component" -> "participant")
        new ParticipantMetrics(
          histograms.participant,
          generateMetricsFactory(
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
        val labeledMetricsFactory = generateMetricsFactory(
          sequencerMetricsContext
        )
        new SequencerMetrics(
          histograms.sequencer,
          labeledMetricsFactory,
          new DamlGrpcServerMetrics(labeledMetricsFactory, "sequencer"),
          new HealthMetrics(labeledMetricsFactory),
        )
      },
    )
  }

  def forMediator(name: String): MediatorMetrics = {
    mediators.getOrElseUpdate(
      name, {
        val mediatorMetricsContext = MetricsContext("node" -> name, "component" -> "mediator")
        val labeledMetricsFactory =
          generateMetricsFactory(mediatorMetricsContext)
        new MediatorMetrics(
          histograms.mediator,
          labeledMetricsFactory,
          new DamlGrpcServerMetrics(labeledMetricsFactory, "mediator"),
          new HealthMetrics(labeledMetricsFactory),
        )
      },
    )
  }

  override def generateMetricsFactory(
      extraContext: MetricsContext
  ): LabeledMetricsFactory = {
    factoryType match {
      case MetricsFactoryType.InMemory(provider) =>
        provider.generateMetricsFactory(extraContext)
      case MetricsFactoryType.External =>
        new QualificationFilteringMetricsFactory(
          new OpenTelemetryMetricsFactory(
            meter,
            histograms.inventory
              .registered()
              .map(_.name.toString())
              .toSet,
            onlyLogMissingHistograms =
              if (testingSupportAdhocMetrics) Some(logger.underlying) else None,
            globalMetricsContext = extraContext,
          ),
          baseFilter,
        )
    }
  }

  /** returns the documented metrics by possibly creating fake participants / sequencers / mediators */
  def metricsDoc(): (Seq[MetricDoc.Item], Seq[MetricDoc.Item], Seq[MetricDoc.Item]) = {
    // TODO(#17917) resurrect once the metrics docs have been re-enabled
    (Seq.empty, Seq.empty, Seq.empty)
  }

  override def close(): Unit = ()

}

object MetricsRegistry extends LazyLogging {

  def registerReporters(
      config: MetricsConfig,
      loggerFactory: NamedLoggerFactory,
  )(
      sdkMeterProviderBuilder: SdkMeterProviderBuilder
  )(implicit scheduledExecutorService: ScheduledExecutorService): SdkMeterProviderBuilder = {
    def buildPeriodicReader(
        exporter: MetricExporter,
        interval: NonNegativeFiniteDuration,
    ): MetricReader = {
      PeriodicMetricReader
        .builder(exporter)
        .setExecutor(scheduledExecutorService)
        .setInterval(interval.asJava)
        .build()
    }
    config.reporters
      .map {
        case Prometheus(hostname, port, _) =>
          logger.info(s"Exposing metrics for Prometheus on port $hostname:$port")
          PrometheusHttpServer
            .builder()
            .setHost(hostname)
            .setPort(port.unwrap)
            .build()
        case config: Csv =>
          logger.info(s"Starting CsvReporter with interval ${config.interval}")
          buildPeriodicReader(new CsvReporter(config, loggerFactory), config.interval)
        case config: Logging =>
          logger.info(s"Starting to log metrics with interval ${config.interval}")
          buildPeriodicReader(
            new LogReporter(logAsInfo = config.logAsInfo, loggerFactory),
            config.interval,
          )

      }
      .zip(config.reporters)
      .foreach { case (reader, config) =>
        sdkMeterProviderBuilder
          .registerMetricReader(FilteringMetricsReader.create(config.filters, reader))
          .discard
      }
    sdkMeterProviderBuilder
  }

}
