// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Path, Paths}

import com.codahale.metrics
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}

import scopt.Read

sealed abstract class MetricsReporter {
  def register(registry: MetricRegistry): ScheduledReporter
}

object MetricsReporter {

  case object Console extends MetricsReporter {
    override def register(registry: MetricRegistry): ScheduledReporter =
      metrics.ConsoleReporter
        .forRegistry(registry)
        .build()
  }

  final case class Csv(directory: Path) extends MetricsReporter {
    override def register(registry: MetricRegistry): ScheduledReporter =
      metrics.CsvReporter
        .forRegistry(registry)
        .build(directory.toFile)
  }

  final case class Graphite(address: InetSocketAddress, prefix: Option[String] = None)
      extends MetricsReporter {
    override def register(registry: MetricRegistry): ScheduledReporter =
      metrics.graphite.GraphiteReporter
        .forRegistry(registry)
        .prefixedWith(prefix.orNull)
        .build(new metrics.graphite.Graphite(address))
  }

  object Graphite {
    val defaultHost: InetAddress = InetAddress.getLoopbackAddress
    val defaultPort: Int = 2003

    def apply(): Graphite =
      Graphite(new InetSocketAddress(defaultHost, defaultPort))

    def apply(port: Int): Graphite =
      Graphite(new InetSocketAddress(defaultHost, port))
  }

  implicit val metricsReporterRead: Read[MetricsReporter] = Read.reads { value =>
    if (value == "console") {
      Console
    } else {
      val uri = Read.uriRead.reads(value)
      uri.getScheme match {
        case "csv" => Csv(Paths.get(uri.getPath))
        case "graphite" =>
          val port = if (uri.getPort > 0) uri.getPort else Graphite.defaultPort
          val address = new InetSocketAddress(uri.getHost, port)
          val metricPrefix = Some(uri.getPath.stripPrefix("/")).filter(_.nonEmpty)
          Graphite(address, metricPrefix)
        case _ =>
          throw new InvalidConfigException(
            """Must be one of "console", "csv:///PATH", or "graphite://HOST[:PORT][/METRIC_PREFIX]".""")
      }
    }
  }
}
