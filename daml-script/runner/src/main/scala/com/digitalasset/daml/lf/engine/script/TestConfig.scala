// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration

import java.io.File
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode

import java.nio.file.Path

case class TestConfig(
    darPath: File,
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    participantConfig: Option[File],
    timeMode: ScriptTimeMode,
    maxInboundMessageSize: Int,
    accessTokenFile: Option[Path],
    tlsConfig: TlsConfiguration,
    applicationId: Option[ApplicationId],
)
