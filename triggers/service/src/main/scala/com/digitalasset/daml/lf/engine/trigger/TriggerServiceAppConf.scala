// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.Uri
import ch.qos.logback.classic.Level
import com.daml.dbutils.JdbcConfig
import com.daml.lf.speedy.Compiler
import com.daml.platform.services.time.TimeProviderType
import pureconfig.{ConfigReader, ConvertHelpers}
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.pureconfigutils.LedgerApiConfig
import com.daml.pureconfigutils.SharedConfigReaders._
import pureconfig.error.FailureReason
import pureconfig.generic.semiauto.deriveReader

import java.nio.file.Path
import java.time.Duration
import scala.concurrent.duration.FiniteDuration

private[trigger] object AuthorizationConfig {
  final case object AuthConfigFailure extends FailureReason {
    val description =
      "You must specify either just auth-common-uri or both auth-internal-uri and auth-external-uri"
  }

  def isValid(ac: AuthorizationConfig): Boolean = {
    (ac.authCommonUri.isDefined && ac.authExternalUri.isEmpty && ac.authInternalUri.isEmpty) ||
    (ac.authCommonUri.isEmpty && ac.authExternalUri.nonEmpty && ac.authInternalUri.nonEmpty)
  }

  implicit val redirectToLoginCfgReader: ConfigReader[AuthClient.RedirectToLogin] =
    ConfigReader.fromString[AuthClient.RedirectToLogin](
      ConvertHelpers.catchReadError(s => Cli.redirectToLogin(s))
    )

  implicit val authCfgReader: ConfigReader[AuthorizationConfig] =
    deriveReader[AuthorizationConfig].emap { ac =>
      Either.cond(isValid(ac), ac, AuthConfigFailure)
    }
}
private[trigger] final case class AuthorizationConfig(
    authInternalUri: Option[Uri] = None,
    authExternalUri: Option[Uri] = None,
    authCommonUri: Option[Uri] = None,
    authRedirect: AuthClient.RedirectToLogin = AuthClient.RedirectToLogin.No,
    authCallbackUri: Option[Uri] = None,
    maxPendingAuthorizations: Int = Cli.DefaultMaxAuthCallbacks,
    authCallbackTimeout: FiniteDuration = Cli.DefaultAuthCallbackTimeout,
)

private[trigger] object TriggerServiceAppConf {
  implicit val compilerCfgReader: ConfigReader[Compiler.Config] =
    ConfigReader.fromString[Compiler.Config](ConvertHelpers.catchReadError { s =>
      s.toLowerCase() match {
        case "default" => Compiler.Config.Default
        case "dev" => Compiler.Config.Dev
        case s =>
          throw new IllegalArgumentException(
            s"Value '$s' for compiler-config is not one of 'default' or 'dev'"
          )
      }
    })

  implicit val levelRead: ConfigReader[Level] =
    ConfigReader.fromString[Level](level => Right(Level.valueOf(level)))

  implicit val serviceCfgReader: ConfigReader[TriggerServiceAppConf] =
    deriveReader[TriggerServiceAppConf]
}

/* An intermediate config representation allowing us to define our HOCON config in a more modular fashion,
  this eventually gets mapped to `ServiceConfig`
 */
private[trigger] final case class TriggerServiceAppConf(
    darPaths: List[Path] = Nil,
    address: String = "127.0.0.1",
    port: Int = Cli.DefaultHttpPort,
    portFile: Option[Path] = None,
    ledgerApi: LedgerApiConfig,
    authorization: AuthorizationConfig = AuthorizationConfig(),
    maxInboundMessageSize: Int = Cli.DefaultMaxInboundMessageSize,
    minRestartInterval: FiniteDuration = Cli.DefaultMinRestartInterval,
    maxRestartInterval: FiniteDuration = Cli.DefaultMaxRestartInterval,
    maxHttpEntityUploadSize: Long = Cli.DefaultMaxHttpEntityUploadSize,
    httpEntityUploadTimeout: FiniteDuration = Cli.DefaultHttpEntityUploadTimeout,
    timeProviderType: TimeProviderType = TimeProviderType.WallClock,
    ttl: FiniteDuration = Cli.DefaultCommandTtl,
    initDb: Boolean = false,
    triggerStore: Option[JdbcConfig] = None,
    allowExistingSchema: Boolean = false,
    compilerConfig: Compiler.Config = Compiler.Config.Default,
    rootLoggingLevel: Option[Level] = None,
) {
  def toServiceConfig: ServiceConfig = {
    ServiceConfig(
      darPaths = darPaths,
      address = address,
      httpPort = port,
      ledgerHost = ledgerApi.address,
      ledgerPort = ledgerApi.port,
      authInternalUri = authorization.authInternalUri,
      authExternalUri = authorization.authExternalUri,
      authBothUri = authorization.authCommonUri,
      authRedirectToLogin = authorization.authRedirect,
      authCallbackUri = authorization.authCallbackUri,
      maxInboundMessageSize = maxInboundMessageSize,
      minRestartInterval = minRestartInterval,
      maxRestartInterval = maxRestartInterval,
      maxAuthCallbacks = authorization.maxPendingAuthorizations,
      authCallbackTimeout = authorization.authCallbackTimeout,
      maxHttpEntityUploadSize = maxHttpEntityUploadSize,
      httpEntityUploadTimeout = httpEntityUploadTimeout,
      timeProviderType = timeProviderType,
      commandTtl =
        Duration.ofSeconds(ttl.toSeconds), // mapping from FiniteDuration to java.time.Duration
      init = initDb,
      jdbcConfig = triggerStore,
      portFile = portFile,
      allowExistingSchema = allowExistingSchema,
      compilerConfig = compilerConfig,
      rootLoggingLevel = rootLoggingLevel,
    )
  }
}
