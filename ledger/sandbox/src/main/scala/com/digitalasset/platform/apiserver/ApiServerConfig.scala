// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.io.File
import java.nio.file.Path

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.ledger.api.tls.TlsConfiguration

case class ApiServerConfig(
    participantId: ParticipantId,
    archiveFiles: List[File],
    port: Int,
    address: Option[String], // address for ledger-api server to bind to, defaulting to `localhost` for None
    jdbcUrl: String,
    tlsConfig: Option[TlsConfiguration],
    maxInboundMessageSize: Int,
    portFile: Option[Path],
)
