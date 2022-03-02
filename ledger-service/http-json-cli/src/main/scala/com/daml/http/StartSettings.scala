// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Path

import com.daml.ledger.api.tls.TlsConfiguration

import scala.concurrent.duration._

import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.cliopts.Logging.LogEncoder
import com.daml.metrics.MetricsReporter

// defined separately from Config so
//  1. it is absolutely lexically apparent what `import startSettings._` means
//  2. avoid incorporating other Config'd things into "the shared args to start"
trait StartSettings {
  val ledgerHost: String
  val ledgerPort: Int
  val address: String
  val httpPort: Int
  val portFile: Option[Path]
  val tlsConfig: TlsConfiguration
  val wsConfig: Option[WebsocketConfig]
  val allowNonHttps: Boolean
  val staticContentConfig: Option[StaticContentConfig]
  val packageReloadInterval: FiniteDuration
  val packageMaxInboundMessageSize: Option[Int]
  val maxInboundMessageSize: Int
  val healthTimeoutSeconds: Int
  val nonRepudiation: nonrepudiation.Configuration.Cli
  val logLevel: Option[LogLevel]
  val logEncoder: LogEncoder
  val metricsReporter: Option[MetricsReporter]
  val metricsReportingInterval: FiniteDuration
}

object StartSettings {

  val DefaultPackageReloadInterval: FiniteDuration = 5.seconds
  val DefaultMaxInboundMessageSize: Int = 4194304
  val DefaultHealthTimeoutSeconds: Int = 5
  val DefaultMetricsReportingInterval: FiniteDuration = 10.seconds

  trait Default extends StartSettings {
    override val staticContentConfig: Option[StaticContentConfig] = None
    override val packageReloadInterval: FiniteDuration = DefaultPackageReloadInterval
    override val packageMaxInboundMessageSize: Option[Int] = None
    override val maxInboundMessageSize: Int = DefaultMaxInboundMessageSize
    override val healthTimeoutSeconds: Int = DefaultHealthTimeoutSeconds
  }
}
