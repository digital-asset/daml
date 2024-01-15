// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.app

import java.net.{InetAddress, InetSocketAddress}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object Configuration {

  private val LocalHost = InetAddress.getLocalHost.getHostAddress
  private val LoopbackAddress = InetAddress.getLoopbackAddress.getHostAddress

  private val ApiDefaultPort: Int = 7882 // Non-repudiation -> NR -> N = 78 and R = 82 in ASCII
  private val ParticipantDefaultPort: Int = 6865
  private val ProxyDefaultPort: Int = ParticipantDefaultPort

  val Default: Configuration =
    Configuration(
      participantAddress = new InetSocketAddress(LocalHost, ParticipantDefaultPort),
      proxyAddress = new InetSocketAddress(LoopbackAddress, ProxyDefaultPort),
      apiAddress = new InetSocketAddress(LoopbackAddress, ApiDefaultPort),
      apiShutdownTimeout = 10.seconds,
      databaseJdbcUrl = "jdbc:postgresql:/",
      databaseJdbcUsername = "nonrepudiation",
      databaseJdbcPassword = "nonrepudiation",
      databaseMaxPoolSize = 10,
      metricsReportingPeriod = 5.seconds,
    )

}

final case class Configuration private (
    participantAddress: InetSocketAddress,
    proxyAddress: InetSocketAddress,
    apiAddress: InetSocketAddress,
    apiShutdownTimeout: FiniteDuration,
    databaseJdbcUrl: String,
    databaseJdbcUsername: String,
    databaseJdbcPassword: String,
    databaseMaxPoolSize: Int,
    metricsReportingPeriod: FiniteDuration,
)
