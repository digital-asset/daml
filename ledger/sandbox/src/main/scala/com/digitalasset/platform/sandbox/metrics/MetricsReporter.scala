// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.Path

sealed trait MetricsReporter

object MetricsReporter {

  case object Console extends MetricsReporter

  final case class Csv(directory: Path) extends MetricsReporter

  final case class Graphite(address: InetSocketAddress) extends MetricsReporter

  object Graphite {
    lazy val defaultHost: InetAddress = InetAddress.getLoopbackAddress
    lazy val defaultPort: Int = 2003

    def apply(): Graphite =
      Graphite(new InetSocketAddress(defaultHost, defaultPort))

    def apply(port: Int): Graphite =
      Graphite(new InetSocketAddress(defaultHost, port))
  }

}
