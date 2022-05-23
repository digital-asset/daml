// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.{IndexConfiguration, InitialLedgerConfiguration}
import com.daml.ports.Port

import java.nio.file.Path
import java.time.Duration
import com.daml.platform.usermanagement.{RateLimitingConfig, UserManagementConfig}

import scala.concurrent.duration.FiniteDuration

case class ApiServerConfig(
    participantId: Ref.ParticipantId,
    port: Port,
    address: Option[String], // This defaults to "localhost" when set to `None`.
    jdbcUrl: String,
    databaseConnectionPoolSize: Int,
    databaseConnectionTimeout: FiniteDuration,
    tlsConfig: Option[TlsConfiguration],
    maxInboundMessageSize: Int,
    initialLedgerConfiguration: Option[InitialLedgerConfiguration],
    configurationLoadTimeout: Duration,
    indexConfiguration: IndexConfiguration,
    portFile: Option[Path],
    seeding: Seeding,
    managementServiceTimeout: Duration,
    userManagementConfig: UserManagementConfig,
    rateLimitingConfig: Option[RateLimitingConfig]
)
