// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.nio.file.Path
import akka.http.scaladsl.model.Uri
import com.daml.auth.middleware.oauth2.Config._
import com.daml.cliopts
import com.daml.jwt.JwtVerifierBase
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
) {
  def validate: Unit = {
    require(oauthToken != null, "Oauth token value on config cannot be null")
    require(oauthAuth != null, "Oauth auth value on config cannot be null")
    require(clientId.nonEmpty, "DAML_CLIENT_ID cannot be empty")
    require(clientSecret.value.nonEmpty, "DAML_CLIENT_SECRET cannot be empty")
    require(tokenVerifier != null, "token verifier must be defined")
  }
}

final case class SecretString(value: String) {
  override def toString: String = "###"
}

object Config {
  val DefaultHttpPort: Int = 3000
  val DefaultCookieSecure: Boolean = true
  val DefaultMaxLoginRequests: Int = 100
  val DefaultLoginTimeout: FiniteDuration = 5.minutes

  implicit val clientSecretReader: ConfigReader[SecretString] =
    ConfigReader.fromString[SecretString](ConvertHelpers.catchReadError(s => SecretString(s)))
  implicit val cfgReader: ConfigReader[Config] = deriveReader[Config]
}
