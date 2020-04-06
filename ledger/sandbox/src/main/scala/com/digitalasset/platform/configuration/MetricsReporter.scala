// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Path, Paths}

import com.codahale.metrics
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
import com.daml.platform.sandbox.config.InvalidConfigException
import com.google.common.net.HostAndPort
import scopt.Read

import scala.util.Try

sealed trait MetricsReporter {
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

  final case class Graphite(address: InetSocketAddress) extends MetricsReporter {
    override def register(registry: MetricRegistry): ScheduledReporter =
      metrics.graphite.GraphiteReporter
        .forRegistry(registry)
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

  implicit val metricsReporterRead: Read[MetricsReporter] = Read.reads {
    _.split(":", 2).toSeq match {
      case Seq("console") => Console
      case Seq("csv", directory) => Csv(Paths.get(directory))
      case Seq("graphite") =>
        Graphite()
      case Seq("graphite", address) =>
        Try(address.toInt)
          .map(port => Graphite(port))
          .recover {
            case _: NumberFormatException =>
              //noinspection UnstableApiUsage
              val hostAndPort = HostAndPort
                .fromString(address)
                .withDefaultPort(Graphite.defaultPort)
              Graphite(new InetSocketAddress(hostAndPort.getHost, hostAndPort.getPort))
          }
          .get
      case _ =>
        throw new InvalidConfigException(
          """Must be one of "console", "csv:PATH", or "graphite[:HOST][:PORT]".""")
    }
  }

}
