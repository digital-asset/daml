// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.configuration.{
  CommandConfiguration,
  InitialLedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port

import java.nio.file.Path
import scala.concurrent.duration._

case class ApiServerConfig(
    address: Option[String] =
      ApiServerConfig.DefaultAddress, // This defaults to "localhost" when set to `None`.
    apiStreamShutdownTimeout: Duration = ApiServerConfig.DefaultApiStreamShutdownTimeout,
    command: CommandConfiguration = ApiServerConfig.DefaultCommand,
    configurationLoadTimeout: Duration = ApiServerConfig.DefaultConfigurationLoadTimeout,
    initialLedgerConfiguration: Option[InitialLedgerConfiguration] =
      ApiServerConfig.DefaultInitialLedgerConfiguration,
    managementServiceTimeout: FiniteDuration = ApiServerConfig.DefaultManagementServiceTimeout,
    maxInboundMessageSize: Int = ApiServerConfig.DefaultMaxInboundMessageSize,
    party: PartyConfiguration = ApiServerConfig.DefaultParty,
    port: Port = ApiServerConfig.DefaultPort,
    portFile: Option[Path] = ApiServerConfig.DefaultPortFile,
    rateLimitingConfig: Option[RateLimitingConfig] = ApiServerConfig.DefaultRateLimitingConfig,
    seeding: Seeding = ApiServerConfig.DefaultSeeding,
    timeProviderType: TimeProviderType = ApiServerConfig.DefaultTimeProviderType,
    tls: Option[TlsConfiguration] = ApiServerConfig.DefaultTls,
    userManagement: UserManagementConfig = ApiServerConfig.DefaultUserManagement,
)

object ApiServerConfig {
  val DefaultPort: Port = Port(6865)
  val DefaultAddress: Option[String] = None
  val DefaultTls: Option[TlsConfiguration] = None
  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024
  val DefaultInitialLedgerConfiguration: Option[InitialLedgerConfiguration] = None
  val DefaultConfigurationLoadTimeout: Duration = 10.seconds
  val DefaultPortFile: Option[Path] = None
  val DefaultSeeding: Seeding = Seeding.Strong
  val DefaultManagementServiceTimeout: FiniteDuration = 2.minutes
  val DefaultUserManagement: UserManagementConfig = UserManagementConfig.default(enabled = false)
  val DefaultParty: PartyConfiguration = PartyConfiguration.Default
  val DefaultCommand: CommandConfiguration = CommandConfiguration.Default
  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock
  val DefaultApiStreamShutdownTimeout: FiniteDuration = FiniteDuration(5, "seconds")
  val DefaultRateLimitingConfig: Option[RateLimitingConfig] = Some(RateLimitingConfig.Default)
}
