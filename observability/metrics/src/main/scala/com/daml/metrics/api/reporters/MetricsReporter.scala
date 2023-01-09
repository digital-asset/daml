// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.reporters

import java.net.URI

import scopt.Read

import scala.util.control.NonFatal

sealed abstract class MetricsReporter {}

object MetricsReporter {

  final case class Prometheus(host: String, port: Int) extends MetricsReporter
  case object None extends MetricsReporter
  object Prometheus {
    val defaultPort: Int = 55001
  }

  def parseMetricsReporter(s: String): MetricsReporter = {
    def getAddress(uri: URI, defaultPort: Int) = {
      if (uri.getHost == null) {
        throw invalidRead
      }
      val port = if (uri.getPort > 0) uri.getPort else defaultPort
      uri.getHost -> port
    }
    s match {
      case value if value.startsWith("prometheus://") =>
        val uri = parseUri(value)
        val (host, port) = getAddress(uri, Prometheus.defaultPort)
        Prometheus(host, port)
      case "none" => None
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
