// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.net.{InetSocketAddress, URI}
import java.nio.file.{Path, Paths}

import com.codahale.metrics
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import scopt.Read

import scala.util.control.NonFatal

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
    val defaultPort: Int = 2003
  }

  implicit val metricsReporterRead: Read[MetricsReporter] = {
    Read.reads {
      case "console" =>
        Console
      case value if value.startsWith("csv://") =>
        val uri = parseUri(value)
        if (uri.getHost != null || uri.getPort >= 0) {
          throw invalidRead
        }
        Csv(Paths.get(uri.getPath))
      case value if value.startsWith("graphite://") =>
        val uri = parseUri(value)
        if (uri.getHost == null) {
          throw invalidRead
        }
        val port = if (uri.getPort > 0) uri.getPort else Graphite.defaultPort
        val address = new InetSocketAddress(uri.getHost, port)
        val metricPrefix = Some(uri.getPath.stripPrefix("/")).filter(_.nonEmpty)
        Graphite(address, metricPrefix)
      case _ =>
        throw invalidRead
    }
  }

  def parseUri(value: String): URI =
    try {
      new URI(value)
    } catch {
      case NonFatal(exception) =>
        throw new InvalidConfigException(invalidReadError + " " + exception.getMessage)
    }

  private val invalidReadError: String =
    """Must be one of "console", "csv:///PATH", or "graphite://HOST[:PORT][/METRIC_PREFIX]"."""

  private def invalidRead: InvalidConfigException =
    new InvalidConfigException(invalidReadError)
}
