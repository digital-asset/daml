// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.nio.file.Path

import org.apache.pekko.http.scaladsl.model.Uri
import com.daml.auth.middleware.oauth2.Config._
import com.daml.cliopts
import com.daml.jwt.JwtVerifierBase
import com.daml.metrics.{HistogramDefinition, MetricsConfig}
import com.daml.metrics.api.reporters.MetricsReporter
import com.daml.pureconfigutils.SharedConfigReaders._
import pureconfig.{ConfigReader, ConvertHelpers}
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

final case class Config(
    // Host and port the middleware listens on
    address: String = cliopts.Http.defaultAddress,
    port: Int = DefaultHttpPort,
    portFile: Option[Path] = None,
    // The URI to which the OAuth2 server will redirect after a completed login flow.
    // Must map to the `/cb` endpoint of the auth middleware.
    callbackUri: Option[Uri] = None,
    maxLoginRequests: Int = DefaultMaxLoginRequests,
    loginTimeout: FiniteDuration = DefaultLoginTimeout,
    cookieSecure: Boolean = DefaultCookieSecure,
    // OAuth2 server endpoints
    oauthAuth: Uri,
    oauthToken: Uri,
    // OAuth2 server request templates
    oauthAuthTemplate: Option[Path] = None,
    oauthTokenTemplate: Option[Path] = None,
    oauthRefreshTemplate: Option[Path] = None,
    // OAuth2 client properties
    clientId: String,
    clientSecret: SecretString,
    // Token verification
    tokenVerifier: JwtVerifierBase,
    metricsReporter: Option[MetricsReporter] = None,
    metricsReportingInterval: FiniteDuration = 10.seconds,
    histograms: Seq[HistogramDefinition],
) {
  def validate(): Unit = {
    require(oauthToken != null, "Oauth token value on config cannot be null")
    require(oauthAuth != null, "Oauth auth value on config cannot be null")
    require(clientId.nonEmpty, "DAML_CLIENT_ID cannot be empty")
    require(clientSecret.value.nonEmpty, "DAML_CLIENT_SECRET cannot be empty")
    require(tokenVerifier != null, "token verifier must be defined")
  }
}

@scala.annotation.nowarn("msg=Block result was adapted via implicit conversion")
object FileConfig {
  implicit val clientSecretReader: ConfigReader[SecretString] =
    ConfigReader.fromString[SecretString](ConvertHelpers.catchReadError(s => SecretString(s)))

  implicit val cfgReader: ConfigReader[FileConfig] = deriveReader[FileConfig]
}

final case class FileConfig(
    address: String = cliopts.Http.defaultAddress,
    port: Int = DefaultHttpPort,
    portFile: Option[Path] = None,
    callbackUri: Option[Uri] = None,
    maxLoginRequests: Int = DefaultMaxLoginRequests,
    loginTimeout: FiniteDuration = DefaultLoginTimeout,
    cookieSecure: Boolean = DefaultCookieSecure,
    oauthAuth: Uri,
    oauthToken: Uri,
    oauthAuthTemplate: Option[Path] = None,
    oauthTokenTemplate: Option[Path] = None,
    oauthRefreshTemplate: Option[Path] = None,
    clientId: String,
    clientSecret: SecretString,
    tokenVerifier: JwtVerifierBase,
    metrics: Option[MetricsConfig] = None,
) {
  def toConfig(): Config = Config(
    address = address,
    port = port,
    portFile = portFile,
    callbackUri = callbackUri,
    maxLoginRequests = maxLoginRequests,
    loginTimeout = loginTimeout,
    cookieSecure = cookieSecure,
    oauthAuth = oauthAuth,
    oauthToken = oauthToken,
    oauthAuthTemplate = oauthAuthTemplate,
    oauthTokenTemplate = oauthTokenTemplate,
    oauthRefreshTemplate = oauthRefreshTemplate,
    clientId = clientId,
    clientSecret = clientSecret,
    tokenVerifier = tokenVerifier,
    metricsReporter = metrics.map(_.reporter),
    metricsReportingInterval =
      metrics.map(_.reportingInterval).getOrElse(MetricsConfig.DefaultMetricsReportingInterval),
    histograms = metrics.toList.flatMap(_.histograms),
  )
}

final case class SecretString(value: String) {
  override def toString: String = "###"
}

object Config {
  val DefaultHttpPort: Int = 3000
  val DefaultCookieSecure: Boolean = true
  val DefaultMaxLoginRequests: Int = 100
  val DefaultLoginTimeout: FiniteDuration = 5.minutes
}
