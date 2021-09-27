// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.File
import java.nio.file.Path
import java.time.Duration

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.configuration.{IndexConfiguration, InitialLedgerConfiguration}
import com.daml.ports.Port

import scala.concurrent.duration.FiniteDuration

case class ApiServerConfig(
    participantId: Ref.ParticipantId,
    archiveFiles: List[File],
    port: Port,
    address: Option[String], // This defaults to "localhost" when set to `None`.
    jdbcUrl: String,
    databaseConnectionPoolSize: Int,
    databaseConnectionTimeout: FiniteDuration,
    tlsConfig: Option[TlsConfiguration],
    maxInboundMessageSize: Int,
    initialLedgerConfiguration: Option[InitialLedgerConfiguration],
    configurationLoadTimeout: Duration,
    eventsPageSize: Int = IndexConfiguration.DefaultEventsPageSize,
    eventsProcessingParallelism: Int = IndexConfiguration.DefaultEventsProcessingParallelism,
    portFile: Option[Path],
    seeding: Seeding,
    managementServiceTimeout: Duration,
    // TODO append-only: remove after removing support for the current (mutating) schema
    enableAppendOnlySchema: Boolean,
    maxContractStateCacheSize: Long,
    maxContractKeyStateCacheSize: Long,
    enableMutableContractStateCache: Boolean,
    maxTransactionsInMemoryFanOutBufferSize: Long,
    enableInMemoryFanOutForLedgerApi: Boolean,
    enableErrorCodesV2: Boolean,
)
