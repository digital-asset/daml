// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.reporters

import scopt.Read

import java.net.{InetSocketAddress, URI}
import scala.util.control.NonFatal

sealed abstract class MetricsReporter {
  def register(): Unit
}

object MetricsReporter {

  object Graphite {
    val defaultPort: Int = 2003
  }

  final case class Prometheus(address: InetSocketAddress) extends MetricsReporter {
    override def register(): Unit = ()
  }

  object Prometheus {
    val defaultPort: Int = 55001
  }

  def parseMetricsReporter(s: String): MetricsReporter = {
    def getAddress(uri: URI, defaultPort: Int) = {
      if (uri.getHost == null) {
        throw invalidRead
      }
      val port = if (uri.getPort > 0) uri.getPort else defaultPort
      new InetSocketAddress(uri.getHost, port)
    }
    s match {
      case value if value.startsWith("prometheus://") =>
        val uri = parseUri(value)
        val address = getAddress(uri, Prometheus.defaultPort)
        Prometheus(address)
      case _ =>
        throw invalidRead
    }
  }

  implicit val metricsReporterRead: Read[MetricsReporter] = {
    Read.reads(parseMetricsReporter)
  }

  val cliHint: String =
    """Must be in the format "prometheus://HOST[:PORT]"."""

  def parseUri(value: String): URI =
    try {
      new URI(value)
    } catch {
      case NonFatal(exception) =>
        throw new RuntimeException(cliHint, exception)
    }

  private def invalidRead: RuntimeException =
    new RuntimeException(cliHint)
}
