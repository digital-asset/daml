// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.stream.ThrottleMode
import com.digitalasset.canton.http.WebsocketConfig as WSC
import scalaz.Show

import scala.concurrent.duration.*

// The internal transient scopt structure *and* StartSettings; external `start`
// users should extend StartSettings or DefaultStartSettings themselves
final case class JsonApiConfig(
    server: HttpServerConfig,
    allowInsecureTokens: Boolean = false,
    websocketConfig: Option[WebsocketConfig] = None,
    debugLoggingOfHttpBodies: Boolean = false,
    damlDefinitionsServiceEnabled: Boolean = false,
  ) extends StartSettings


// It is public for Daml Hub
final case class WebsocketConfig(
    maxDuration: FiniteDuration = WSC.DefaultMaxDuration,
    throttleElem: Int = WSC.DefaultThrottleElem,
    throttlePer: FiniteDuration = WSC.DefaultThrottlePer,
    maxBurst: Int = WSC.DefaultMaxBurst,
    mode: ThrottleMode = WSC.DefaultThrottleMode,
    heartbeatPeriod: FiniteDuration = WSC.DefaultHeartbeatPeriod,
)

object WebsocketConfig {
  implicit val showInstance: Show[WebsocketConfig] = Show.shows(c =>
    s"WebsocketConfig(maxDuration=${c.maxDuration}, heartBeatPer=${c.heartbeatPeriod})"
  )

  val DefaultMaxDuration: FiniteDuration = 120.minutes
  val DefaultThrottleElem: Int = 20
  val DefaultThrottlePer: FiniteDuration = 1.second
  val DefaultMaxBurst: Int = 20
  val DefaultThrottleMode: ThrottleMode = ThrottleMode.Shaping
  val DefaultHeartbeatPeriod: FiniteDuration = 5.second
}
