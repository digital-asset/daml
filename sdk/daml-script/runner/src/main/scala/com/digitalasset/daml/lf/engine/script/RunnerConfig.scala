// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.nio.file.Path
import java.io.File

import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode

case class RunnerConfig(
    darPath: File,
    scriptIdentifier: String,
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    participantConfig: Option[File],
    timeMode: ScriptTimeMode,
    inputFile: Option[File],
    outputFile: Option[File],
    accessTokenFile: Option[Path],
    tlsConfig: TlsConfiguration,
    jsonApi: Boolean,
    maxInboundMessageSize: Int,
    // While we do have a default application id, we
    // want to differentiate between not specifying the application id
    // and specifying the default for better error messages.
    applicationId: Option[ApplicationId],
)
