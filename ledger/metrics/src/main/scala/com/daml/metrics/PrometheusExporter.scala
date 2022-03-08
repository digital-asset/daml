// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.ledger.resources.ResourceOwner
import io.prometheus.client.exporter.HTTPServer
import scopt.Read

import java.net.{InetSocketAddress, URI}
import scala.util.control.NonFatal

sealed trait MetricsReporter

object MetricsReporter {
  implicit val metricsReporterRead: Read[MetricsReporter] =
    Read.reads(Cli.parseReporter)

  def owner(reporter: MetricsReporter): ResourceOwner[Unit] = {
    reporter match {
      case PrometheusReporter(address) =>
        PrometheusServer.owner(address).map(_ => ())
    }
  }

  object Cli {
    // TODO: Prometheus metrics: Graphite bridge
    val Hint: String =
      """Must be one of "graphite://HOST[:PORT][/METRIC_PREFIX]" or "prometheus://HOST[:PORT]"."""

    def parseReporter(s: String): MetricsReporter = {
      s match {
        case value if value.startsWith("prometheus://") =>
          val uri = parseUri(value)
          val address = getAddress(uri, PrometheusServer.DefaultPort)
          PrometheusReporter(address)
        case _ =>
          throw invalidRead
      }
    }

    private def parseUri(value: String): URI =
      try {
        new URI(value)
      } catch {
        case NonFatal(exception) =>
          throw new RuntimeException(Hint, exception)
      }

    private def getAddress(uri: URI, defaultPort: Int) = {
      if (uri.getHost == null) {
        throw invalidRead
      }
      val port = if (uri.getPort > 0) uri.getPort else defaultPort
      new InetSocketAddress(uri.getHost, port)
    }

    private def invalidRead: RuntimeException =
      new RuntimeException(Hint)
  }

  final case class PrometheusReporter(address: InetSocketAddress) extends MetricsReporter
  object PrometheusReporter {
    val Default: PrometheusReporter = PrometheusReporter(
      new InetSocketAddress("localhost", PrometheusServer.DefaultPort)
    )
  }
}

object PrometheusServer {
  val DefaultPort: Int = 55001

  def owner(address: InetSocketAddress): ResourceOwner[HTTPServer] =
    ResourceOwner.forCloseable(() => builder(address).build())

  private def builder(address: InetSocketAddress): HTTPServer.Builder =
    new HTTPServer.Builder()
      .withHostname(address.getHostName)
      .withPort(address.getPort)
}
