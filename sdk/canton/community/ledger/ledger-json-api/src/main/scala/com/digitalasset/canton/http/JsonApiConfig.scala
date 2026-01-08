// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.http.WebsocketConfig as WSC
import org.apache.pekko.stream.ThrottleMode
import scalaz.Show

import java.nio.file.Path
import scala.concurrent.duration.*

// The internal transient scopt structure *and* StartSettings; external `start`
// users should extend StartSettings or DefaultStartSettings themselves
final case class JsonApiConfig(
    enabled: Boolean = true,
    websocketConfig: Option[WebsocketConfig] = None,
    //  @deprecated Use ApiLoggingConfig to enable HTTP body logging
    debugLoggingOfHttpBodies: Boolean = false,
    damlDefinitionsServiceEnabled: Boolean = false,
    address: String = JsonApiConfig.defaultAddress,
    internalPort: Option[Port] = None,
    portFile: Option[Path] = None,
    pathPrefix: Option[String] = None,
    requestTimeout: FiniteDuration = JsonApiConfig.defaultRequestTimeout,
) {
  def port: Port =
    internalPort.getOrElse(
      throw new IllegalStateException("Accessing server port before default was set")
    )
}

object JsonApiConfig {
  private val defaultAddress: String = java.net.InetAddress.getLoopbackAddress.getHostAddress
  private val defaultRequestTimeout: FiniteDuration = 20.seconds
}

final case class WebsocketConfig(
    maxDuration: FiniteDuration = WSC.DefaultMaxDuration, // v1 only
    throttleElem: Int = WSC.DefaultThrottleElem, // v1 only
    throttlePer: FiniteDuration = WSC.DefaultThrottlePer, // v1 only
    maxBurst: Int = WSC.DefaultMaxBurst, // v1 only
    mode: ThrottleMode = WSC.DefaultThrottleMode, // v1 only
    heartbeatPeriod: FiniteDuration = WSC.DefaultHeartbeatPeriod, // v1 only
    httpListMaxElementsLimit: Long = WSC.DefaultHttpListMaxElementsLimit,
    httpListWaitTime: FiniteDuration = WSC.DefaultHttpListWaitTime,
)

object WebsocketConfig {
  implicit val showInstance: Show[WebsocketConfig] = Show.shows(c =>
    s"WebsocketConfig(httpListMaxElementsLimit=${c.httpListMaxElementsLimit}, httpListWaitTime=${c.httpListWaitTime})"
  )

  val DefaultMaxDuration: FiniteDuration = 120.minutes
  val DefaultThrottleElem: Int = 20
  val DefaultThrottlePer: FiniteDuration = 1.second
  val DefaultMaxBurst: Int = 20
  val DefaultThrottleMode: ThrottleMode = ThrottleMode.Shaping
  val DefaultHeartbeatPeriod: FiniteDuration = 5.second
  // Canton transactions can be quite big (20kb) so we keep max number of returned transactions in list low by default
  val DefaultHttpListMaxElementsLimit: Long = 200
  val DefaultHttpListWaitTime: FiniteDuration = 0.5.seconds
}
