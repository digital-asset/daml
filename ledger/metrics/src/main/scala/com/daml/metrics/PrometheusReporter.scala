// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import java.net.InetSocketAddress
import java.util
import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.client.exporter.HTTPServer
import org.slf4j.LoggerFactory

object PrometheusReporter {

  def forRegistry(registry: MetricRegistry) = PrometheusReporter.Builder(registry)

  case class Builder(registry: MetricRegistry, filter: MetricFilter = MetricFilter.ALL) {
    def withFilter(filter: MetricFilter): Builder = copy(filter = filter)
    def build(address: InetSocketAddress) = new PrometheusReporter(registry, filter, address)
  }

}

final class PrometheusReporter private (
    registry: MetricRegistry,
    filter: MetricFilter,
    address: InetSocketAddress,
) extends ScheduledReporter(
      registry,
      "prometheus-reporter",
      filter,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS,
    ) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val server = new HTTPServer(address.getHostName, address.getPort)
  private val exports = new ExtendedDropwizardExports(registry).register[DropwizardExports]()
  logger.info(s"Reporting prometheus metrics on ${address}")

  override def report(
      gauges: util.SortedMap[String, Gauge[_]],
      counters: util.SortedMap[String, Counter],
      histograms: util.SortedMap[String, Histogram],
      meters: util.SortedMap[String, Meter],
      timers: util.SortedMap[String, Timer],
  ): Unit = {
    // PrometheusReporter does nothing in the standard report callback
    // Prometheus infrastructure runs its own background thread that periodically
    // pokes DropwizardExports for new metrics
  }

  override def stop(): Unit = {
    server.stop()
    CollectorRegistry.defaultRegistry.unregister(exports)
    super.stop()
  }
}
