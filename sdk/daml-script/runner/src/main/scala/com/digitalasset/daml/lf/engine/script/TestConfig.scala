// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.nio.file.Path
import java.io.File

import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration

import com.daml.lf.engine.script.ScriptTimeMode

case class TestConfig(
    darPath: File,
    participantMode: ParticipantMode,
    timeMode: ScriptTimeMode,
    maxInboundMessageSize: Int,
    accessTokenFile: Option[Path],
    tlsConfig: TlsConfiguration,
    applicationId: Option[ApplicationId],
)
