// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.io.File
import java.nio.file.Path

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.tls.TlsConfiguration

case class ApiServerConfig(
    participantId: ParticipantId,
    archiveFiles: List[File],
    port: Int,
    jdbcUrl: String,
    tlsConfig: Option[TlsConfiguration],
    timeProvider: TimeProvider, // enables use of non-wall-clock time in tests
    maxInboundMessageSize: Int,
    portFile: Option[Path],
)
