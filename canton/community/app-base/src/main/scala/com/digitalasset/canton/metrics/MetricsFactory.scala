// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics
import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.daml.metrics.{
  ExecutorServiceMetrics,
  HealthMetrics as DMHealth,
  HistogramDefinition,
  JvmMetricSet,
}
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.domain.metrics.{
  DomainMetrics,
  EnvMetrics,
  MediatorNodeMetrics,
  SequencerMetrics,
}
import com.digitalasset.canton.metrics.MetricHandle.{
  CantonDropwizardMetricsFactory,
  CantonOpenTelemetryMetricsFactory,
}
import com.digitalasset.canton.metrics.MetricsConfig.MetricsFilterConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import io.prometheus.client.dropwizard.DropwizardExports

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap

final case class MetricsConfig(
    reporters: Seq[MetricsReporterConfig] = Seq.empty,
    reportJvmMetrics: Boolean = false,
    reportExecutionContextMetrics: Boolean = false,
    histograms: Seq[HistogramDefinition] = Seq.empty,
)

sealed trait MetricsReporterConfig {
  def filters: Seq[MetricsFilterConfig]

  def metricFilter: MetricFilter =
    (name: String, _: Metric) => filters.isEmpty || filters.exists(_.matches(name))
}

sealed trait MetricsPrefix
object MetricsPrefix {

  /** Do not use a prefix */
  object NoPrefix extends MetricsPrefix

  /** Use a static text string as prefix */
  final case class Static(prefix: String) extends MetricsPrefix

  /** Uses the hostname as the prefix */
  object Hostname extends MetricsPrefix

  def prefixFromConfig(prefix: MetricsPrefix): Option[String] = prefix match {
    case Hostname => Some(java.net.InetAddress.getLocalHost.getHostName)
    case NoPrefix => None
    case Static(prefix) => Some(prefix)
  }
}

object MetricsConfig {

  final case class JMX(filters: Seq[MetricsFilterConfig] = Seq.empty) extends MetricsReporterConfig

  final case class Csv(
      directory: File,
      interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
  ) extends MetricsReporterConfig

  final case class Graphite(
      address: String = "localhost",
      port: Int = 2003,
      prefix: MetricsPrefix = MetricsPrefix.Hostname,
      interval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
      filters: Seq[MetricsFilterConfig] = Seq.empty,
  ) extends MetricsReporterConfig

  final case class Prometheus(address: String = "localhost", port: Int = 9100)
      extends MetricsReporterConfig {
    override def filters: Seq[MetricsFilterConfig] = Seq.empty
  }

  final case class MetricsFilterConfig(
      startsWith: String = "",
      contains: String = "",
      endsWith: String = "",
  ) {
    def matches(name: String): Boolean =
      name.startsWith(startsWith) && name.contains(contains) && name.endsWith(endsWith)
  }
}

final case class MetricsFactory(
    reporters: Seq[metrics.Reporter],
    registry: metrics.MetricRegistry,
    reportJVMMetrics: Boolean,
    meter: Meter,
    factoryType: MetricsFactoryType,
    reportExecutionContextMetrics: Boolean,
) extends AutoCloseable {

  val metricsFactory: MetricHandle.LabeledMetricsFactory =
    createDropwizardMetricsFactory(MetricsContext.Empty, registry)
  private val envMetrics = new EnvMetrics(metricsFactory)
  private val participants = TrieMap[String, ParticipantMetrics]()
  private val domains = TrieMap[String, DomainMetrics]()
  private val sequencers = TrieMap[String, SequencerMetrics]()
  private val mediators = TrieMap[String, MediatorNodeMetrics]()
  private val allNodeMetrics: Seq[TrieMap[String, ?]] =
    Seq(participants, domains, sequencers, mediators)
  private def nodeMetricsExcept(toExclude: TrieMap[String, ?]): Seq[TrieMap[String, ?]] =
    allNodeMetrics filterNot (_ eq toExclude)

  val executionServiceMetrics: ExecutorServiceMetrics = new ExecutorServiceMetrics(
    createOpenTelemetryMetricsFactory(MetricsContext.Empty)
  )

  object benchmark extends MetricsGroup(MetricName(MetricsFactory.prefix :+ "benchmark"), registry)

  object health extends HealthMetrics(MetricName(MetricsFactory.prefix :+ "health"), registry)

  // add default, system wide metrics to the metrics reporter
  if (reportJVMMetrics) {
    registry.registerAll(new JvmMetricSet) // register Daml repo JvmMetricSet
    JvmMetricSet.registerObservers() // requires OpenTelemetry to have the global lib setup
  }

  private def newRegistry(prefix: String): metrics.MetricRegistry = {
    val nested = new metrics.MetricRegistry()
    registry.register(prefix, nested)
    nested
  }

  def forParticipant(name: String): ParticipantMetrics = {
    participants.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "participant", participants)
        val participantMetricsContext =
          MetricsContext("participant" -> name, "component" -> "participant")
        val participantRegistry = newRegistry(metricName)
        new ParticipantMetrics(
          name,
          MetricsFactory.prefix,
          createDropwizardMetricsFactory(participantMetricsContext, participantRegistry),
          createOpenTelemetryMetricsFactory(
            participantMetricsContext
          ),
          participantRegistry,
          reportExecutionContextMetrics,
        )
      },
    )
  }

  def forEnv: EnvMetrics = envMetrics

  def forDomain(name: String): DomainMetrics = {
    domains.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "domain", domains)
        val domainMetricsContext = MetricsContext("domain" -> name, "component" -> "domain")
        val labeledMetricsFactory =
          createOpenTelemetryMetricsFactory(domainMetricsContext)
        new DomainMetrics(
          MetricsFactory.prefix,
          createDropwizardMetricsFactory(domainMetricsContext, newRegistry(metricName)),
          new DamlGrpcServerMetrics(labeledMetricsFactory, "domain"),
          new DMHealth(labeledMetricsFactory),
        )
      },
    )
  }

  def forSequencer(name: String): SequencerMetrics = {
    sequencers.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "sequencer", sequencers)
        val sequencerMetricsContext =
          MetricsContext("sequencer" -> name, "component" -> "sequencer")
        val labeledMetricsFactory = createOpenTelemetryMetricsFactory(
          sequencerMetricsContext
        )
        new SequencerMetrics(
          MetricsFactory.prefix,
          createDropwizardMetricsFactory(sequencerMetricsContext, newRegistry(metricName)),
          new DamlGrpcServerMetrics(labeledMetricsFactory, "sequencer"),
          new DMHealth(labeledMetricsFactory),
        )
      },
    )
  }

  def forMediator(name: String): MediatorNodeMetrics = {
    mediators.getOrElseUpdate(
      name, {
        val metricName = deduplicateName(name, "mediator", mediators)
        val mediatorMetricsContext = MetricsContext("mediator" -> name, "component" -> "mediator")
        val labeledMetricsFactory =
          createOpenTelemetryMetricsFactory(mediatorMetricsContext)
        new MediatorNodeMetrics(
          MetricsFactory.prefix,
          createDropwizardMetricsFactory(mediatorMetricsContext, newRegistry(metricName)),
          new DamlGrpcServerMetrics(labeledMetricsFactory, "mediator"),
          new DMHealth(labeledMetricsFactory),
        )
      },
    )
  }

  /** de-duplicate name if there is someone using the same name for another type of node (not sure that will ever happen)
    */
  private def deduplicateName(
      name: String,
      nodeType: String,
      nodesToExclude: TrieMap[String, ?],
  ): String =
    if (nodeMetricsExcept(nodesToExclude).exists(_.keySet.contains(name)))
      s"$nodeType-$name"
    else name

  private def createOpenTelemetryMetricsFactory(extraContext: MetricsContext) = {
    factoryType match {
      case MetricsFactoryType.InMemory(provider) =>
        provider(extraContext)
      case MetricsFactoryType.External =>
        new CantonOpenTelemetryMetricsFactory(
          meter,
          globalMetricsContext = MetricsContext(
            "canton_version" -> BuildInfo.version
          ).merge(extraContext),
        )
    }
  }

  private def createDropwizardMetricsFactory(
      extraContext: MetricsContext,
      registry: MetricRegistry,
  ) = factoryType match {
    case MetricsFactoryType.InMemory(builder) => builder(extraContext)
    case MetricsFactoryType.External => new CantonDropwizardMetricsFactory(registry)
  }

  /** returns the documented metrics by possibly creating fake participants / domains */
  def metricsDoc(): (Seq[MetricDoc.Item], Seq[MetricDoc.Item]) = {
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
    val domainMetrics = MetricDoc.getItems(
      domains.headOption
        .map { case (_, domainMetrics) => domainMetrics }
        .getOrElse(forDomain("dummyDomain"))
    )

    // the fake instances are fine here as we do this anyway only when we build and export the docs
    (sorted(participantItems ++ clientMetrics), sorted(domainMetrics))
  }

  override def close(): Unit = reporters.foreach(_.close())

}

object MetricsFactory extends LazyLogging {

  import MetricsConfig.*

  val prefix: MetricName = MetricName("canton")

  def forConfig(
      config: MetricsConfig,
      openTelemetry: OpenTelemetry,
      metricsFactoryType: MetricsFactoryType,
  ): MetricsFactory = {
    val registry = new metrics.MetricRegistry()
    val reporter = registerReporter(config, registry)
    new MetricsFactory(
      reporter,
      registry,
      config.reportJvmMetrics,
      openTelemetry.meterBuilder("canton").build(),
      metricsFactoryType,
      config.reportExecutionContextMetrics,
    )
  }

  private def registerReporter(
      config: MetricsConfig,
      registry: metrics.MetricRegistry,
  ): Seq[metrics.Reporter] = {
    config.reporters.map {

      case reporterConfig @ JMX(_filters) =>
        val reporter =
          metrics.jmx.JmxReporter.forRegistry(registry).filter(reporterConfig.metricFilter).build()
        logger.debug("Starting metrics reporting using JMX")
        reporter.start()
        reporter

      case reporterConfig @ Csv(directory, interval, _filters) =>
        directory.mkdirs()
        logger.debug(s"Starting metrics reporting to csv-file ${directory.toString}")
        val reporter = metrics.CsvReporter
          .forRegistry(registry)
          .filter(reporterConfig.metricFilter)
          .formatFor(Locale.ENGLISH) // Format decimal numbers like "12345.12345".
          .build(directory)
        reporter.start(interval.unwrap.toMillis, TimeUnit.MILLISECONDS)
        reporter

      case reporterConfig @ Graphite(address, port, prefix, interval, _filters) =>
        logger.debug(s"Starting metrics reporting for Graphite to $address:$port")
        val builder = metrics.graphite.GraphiteReporter
          .forRegistry(registry)
          .filter(reporterConfig.metricFilter)
        val reporter = MetricsPrefix
          .prefixFromConfig(prefix)
          .fold(builder)(str => builder.prefixedWith(str))
          .build(new metrics.graphite.Graphite(address, port))
        reporter.start(interval.unwrap.toMillis, TimeUnit.MILLISECONDS)
        reporter

      // OpenTelemetry registers the prometheus collector during initialization
      case Prometheus(hostname, port) =>
        logger.debug(s"Exposing metrics for Prometheus on port $hostname:$port")
        new DropwizardExports(registry).register[DropwizardExports]()
        val reporter = new Reporters.Prometheus(hostname, port)
        reporter
    }
  }
}

class HealthMetrics(prefix: MetricName, registry: metrics.MetricRegistry)
    extends MetricsGroup(prefix, registry) {

  val pingLatency: metrics.Timer = timer("ping-latency")
}

abstract class MetricsGroup(prefix: MetricName, registry: metrics.MetricRegistry) {

  def timer(name: String): metrics.Timer = registry.timer(MetricName(prefix :+ name))
}
