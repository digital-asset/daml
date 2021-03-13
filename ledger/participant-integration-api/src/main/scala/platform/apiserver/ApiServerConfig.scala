// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.File
import java.nio.file.Path
import java.time.Duration

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.configuration.IndexConfiguration
import com.daml.ports.Port

case class ApiServerConfig(
    participantId: ParticipantId,
    archiveFiles: List[File],
    port: Port,
    address: Option[String], // This defaults to "localhost" when set to `None`.
    jdbcUrl: String,
    databaseConnectionPoolSize: Int,
    tlsConfig: Option[TlsConfiguration],
    maxInboundMessageSize: Int,
    eventsPageSize: Int = IndexConfiguration.DefaultEventsPageSize,
    portFile: Option[Path],
    seeding: Seeding,
    managementServiceTimeout: Duration,
    stateCacheSize: Int,
    keyCacheSize: Int,
)
