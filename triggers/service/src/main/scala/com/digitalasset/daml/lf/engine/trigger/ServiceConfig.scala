// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.speedy.Compiler

import java.nio.file.Path
import java.time.Duration
import akka.http.scaladsl.model.Uri
import ch.qos.logback.classic.Level
import com.daml.platform.services.time.TimeProviderType
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.dbutils.JdbcConfig

import scala.concurrent.duration.FiniteDuration

private[trigger] final case class ServiceConfig(
    // For convenience, we allow passing DARs on startup
    // as opposed to uploading them dynamically.
    darPaths: List[Path],
    address: String,
    httpPort: Int,
    ledgerHost: String,
    ledgerPort: Int,
    authInternalUri: Option[Uri],
    authExternalUri: Option[Uri],
    authBothUri: Option[Uri],
    authRedirectToLogin: AuthClient.RedirectToLogin,
    authCallbackUri: Option[Uri],
    maxInboundMessageSize: Int,
    minRestartInterval: FiniteDuration,
    maxRestartInterval: FiniteDuration,
    maxAuthCallbacks: Int,
    authCallbackTimeout: FiniteDuration,
    maxHttpEntityUploadSize: Long,
    httpEntityUploadTimeout: FiniteDuration,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
    init: Boolean,
    jdbcConfig: Option[JdbcConfig],
    portFile: Option[Path],
    allowExistingSchema: Boolean,
    compilerConfig: Compiler.Config,
    triggerConfig: TriggerRunnerConfig,
    rootLoggingLevel: Option[Level],
    logEncoder: LogEncoder,
)
