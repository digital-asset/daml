// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.Uri
import ch.qos.logback.classic.Level
import com.daml.dbutils.JdbcConfig
import com.daml.lf.speedy.Compiler
import com.daml.platform.services.time.TimeProviderType
import pureconfig.{ConfigReader, ConvertHelpers}
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.engine.trigger.TriggerRunnerConfig.DefaultTriggerRunnerConfig
import com.daml.lf.language
import com.daml.lf.language.LanguageMajorVersion
import com.daml.metrics.MetricsConfig
import com.daml.pureconfigutils.LedgerApiConfig
import com.daml.pureconfigutils.SharedConfigReaders._
import pureconfig.error.FailureReason
import pureconfig.generic.semiauto.deriveReader

import java.io.File
import java.nio.file.Path
import java.time.Duration
import scala.concurrent.duration.FiniteDuration

@scala.annotation.nowarn("msg=Block result was adapted via implicit conversion")
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

@scala.annotation.nowarn("msg=Block result was adapted via implicit conversion")
private[trigger] object TriggerServiceAppConf {

  sealed trait CompilerConfigBuilder extends Product with Serializable {
    def build(majorLanguageVersion: LanguageMajorVersion): Compiler.Config
  }
  object CompilerConfigBuilder {
    final case object Default extends CompilerConfigBuilder {
      override def build(majorLanguageVersion: LanguageMajorVersion): Compiler.Config =
        Compiler.Config.Default(majorLanguageVersion)
    }
    final case object Dev extends CompilerConfigBuilder {
      override def build(majorLanguageVersion: LanguageMajorVersion): Compiler.Config =
        Compiler.Config.Dev(majorLanguageVersion)
    }
  }

  implicit val compilerCfgBuilderReader: ConfigReader[CompilerConfigBuilder] =
    ConfigReader.fromString[CompilerConfigBuilder](
      ConvertHelpers.catchReadError { s =>
        s.toLowerCase() match {
          case "default" => CompilerConfigBuilder.Default
          case "dev" => CompilerConfigBuilder.Dev
          case s =>
            throw new IllegalArgumentException(
              s"Value '$s' for compiler-config is not one of 'default' or 'dev'"
            )
        }
      }
    )

  implicit val lfMajorVersionReader: ConfigReader[language.LanguageMajorVersion] =
    ConfigReader.fromStringOpt[language.LanguageMajorVersion](s =>
      LanguageMajorVersion.fromString(s)
    )

  implicit val levelReader: ConfigReader[Level] =
    ConfigReader.fromString[Level](level => Right(Level.valueOf(level)))

  implicit val logEncoderReader: ConfigReader[LogEncoder] =
    ConfigReader.fromString[LogEncoder](ConvertHelpers.catchReadError {
      case "plain" => LogEncoder.Plain
      case "json" => LogEncoder.Json
      case s =>
        throw new IllegalArgumentException(
          s"Value '$s' for log-encoder is not one of 'plain' or 'json'"
        )
    })

  implicit val triggerTlsConfigReader: ConfigReader[TlsConfiguration] =
    ConfigReader.fromCursor { cur =>
      for {
        objCur <- cur.asObjectCursor
        tlsCur <- objCur.atKey("tls")
        enabled <- tlsCur.asBoolean
        pemCur <- objCur.atKey("pem")
        privateKeyFile <- pemCur.asString
        crtCur <- objCur.atKey("crt")
        certChainFile <- crtCur.asString
        cacrtCur <- objCur.atKey("cacrt")
        trustCollectionFile <- cacrtCur.asString
      } yield TlsConfiguration(
        enabled,
        Some(new File(certChainFile)),
        Some(new File(privateKeyFile)),
        Some(new File(trustCollectionFile)),
      )
    }

  implicit val serviceCfgReader: ConfigReader[TriggerServiceAppConf] =
    deriveReader[TriggerServiceAppConf]

  implicit val triggerConfigReader: ConfigReader[TriggerRunnerConfig] =
    deriveReader[TriggerRunnerConfig]

  implicit val triggerRunnerHardLimitsReader: ConfigReader[TriggerRunnerHardLimits] =
    deriveReader[TriggerRunnerHardLimits]
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
    tlsConfig: TlsConfiguration = Cli.DefaultTlsConfiguration,
    compilerConfig: TriggerServiceAppConf.CompilerConfigBuilder =
      TriggerServiceAppConf.CompilerConfigBuilder.Default,
    lfMajorVersion: LanguageMajorVersion = LanguageMajorVersion.V1,
    triggerConfig: TriggerRunnerConfig = DefaultTriggerRunnerConfig,
    rootLoggingLevel: Option[Level] = None,
    logEncoder: LogEncoder = LogEncoder.Plain,
    metrics: Option[MetricsConfig] = None,
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
      tlsConfig = tlsConfig,
      compilerConfig = compilerConfig.build(lfMajorVersion),
      triggerConfig = triggerConfig,
      rootLoggingLevel = rootLoggingLevel,
      logEncoder = logEncoder,
      metricsReporter = metrics.map(_.reporter),
      metricsReportingInterval =
        metrics.map(_.reportingInterval).getOrElse(MetricsConfig.DefaultMetricsReportingInterval),
      histograms = metrics.toList.flatMap(_.histograms),
    )
  }
}
