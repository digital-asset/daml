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
import com.daml.platform.store.DbSupport.{ConnectionPoolConfig, DbConfig}
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port

import java.nio.file.Path
import scala.concurrent.duration._

case class ApiServerConfig(
    port: Port = ApiServerConfig.DefaultPort,
    address: Option[String] =
      ApiServerConfig.DefaultAddress, // This defaults to "localhost" when set to `None`.
    tls: Option[TlsConfiguration] = ApiServerConfig.DefaultTls,
    maxInboundMessageSize: Int = ApiServerConfig.DefaultMaxInboundMessageSize,
    initialLedgerConfiguration: Option[InitialLedgerConfiguration] =
      ApiServerConfig.DefaultInitialLedgerConfiguration,
    configurationLoadTimeout: Duration = ApiServerConfig.DefaultConfigurationLoadTimeout,
    portFile: Option[Path] = ApiServerConfig.DefaultPortFile,
    seeding: Seeding = ApiServerConfig.DefaultSeeding,
    managementServiceTimeout: FiniteDuration = ApiServerConfig.DefaultManagementServiceTimeout,
    userManagement: UserManagementConfig = ApiServerConfig.DefaultUserManagement,
    authentication: AuthServiceConfig = ApiServerConfig.DefaultAuthentication,
    party: PartyConfiguration = ApiServerConfig.DefaultParty,
    command: CommandConfiguration = ApiServerConfig.DefaultCommand,
    timeProviderType: TimeProviderType = ApiServerConfig.DefaultTimeProviderType,
    database: DbConfig = ApiServerConfig.DefaultDatabase,
    rateLimitingConfig: Option[RateLimitingConfig] = ApiServerConfig.DefaultRateLimitingConfig,
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
  val DefaultAuthentication: AuthServiceConfig = AuthServiceConfig.Wildcard
  val DefaultParty: PartyConfiguration = PartyConfiguration.default
  val DefaultCommand: CommandConfiguration = CommandConfiguration.default
  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock
  val DefaultDatabase: DbConfig = DbConfig(
    jdbcUrl = "default-jdbc-url",
    connectionPool = ConnectionPoolConfig(
      minimumIdle = 16,
      maxPoolSize = 16,
      connectionTimeout = 250.millis,
    ),
  )
  val DefaultRateLimitingConfig: Option[RateLimitingConfig] = Some(RateLimitingConfig.default)
}
