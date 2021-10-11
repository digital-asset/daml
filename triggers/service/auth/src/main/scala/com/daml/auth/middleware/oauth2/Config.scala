// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.nio.file.{Path, Paths}

import akka.http.scaladsl.model.Uri
import com.daml.cliopts
import com.daml.jwt.{JwtVerifierBase, JwtVerifierConfigurationCli}

import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration

case class Config(
    // Host and port the middleware listens on
    address: String,
    port: Int,
    portFile: Option[Path],
    // The URI to which the OAuth2 server will redirect after a completed login flow.
    // Must map to the `/cb` endpoint of the auth middleware.
    callbackUri: Option[Uri],
    maxLoginRequests: Int,
    loginTimeout: FiniteDuration,
    cookieSecure: Boolean,
    // OAuth2 server endpoints
    oauthAuth: Uri,
    oauthToken: Uri,
    // OAuth2 server request templates
    oauthAuthTemplate: Option[Path],
    oauthTokenTemplate: Option[Path],
    oauthRefreshTemplate: Option[Path],
    // OAuth2 client properties
    clientId: String,
    clientSecret: SecretString,
    // Token verification
    tokenVerifier: JwtVerifierBase,
)

case class SecretString(value: String) {
  override def toString: String = "###"
}

object Config {
  val DefaultHttpPort: Int = 3000
  val DefaultCookieSecure: Boolean = true
  val DefaultMaxLoginRequests: Int = 100
  val DefaultLoginTimeout: FiniteDuration = FiniteDuration(5, duration.MINUTES)

  private val Empty =
    Config(
      address = cliopts.Http.defaultAddress,
      port = DefaultHttpPort,
      portFile = None,
      callbackUri = None,
      maxLoginRequests = DefaultMaxLoginRequests,
      loginTimeout = DefaultLoginTimeout,
      cookieSecure = DefaultCookieSecure,
      oauthAuth = null,
      oauthToken = null,
      oauthAuthTemplate = None,
      oauthTokenTemplate = None,
      oauthRefreshTemplate = None,
      clientId = null,
      clientSecret = null,
      tokenVerifier = null,
    )

  def parseConfig(args: collection.Seq[String]): Option[Config] =
    configParser.parse(args, Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val configParser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("oauth-middleware") {
      head("OAuth2 Middleware")

      cliopts.Http.serverParse(this, serviceName = "OAuth2 Middleware")(
        address = (f, c) => c.copy(address = f(c.address)),
        httpPort = (f, c) => c.copy(port = f(c.port)),
        defaultHttpPort = Some(DefaultHttpPort),
        portFile = Some((f, c) => c.copy(portFile = f(c.portFile))),
      )

      opt[String]("callback")
        .action((x, c) => c.copy(callbackUri = Some(Uri(x))))
        .text(
          "URI to the auth middleware's callback endpoint `/cb`. By default constructed from the incoming login request."
        )

      opt[Int]("max-pending-login-requests")
        .action((x, c) => c.copy(maxLoginRequests = x))
        .text(
          "Maximum number of simultaneously pending login requests. Requests will be denied when exceeded until earlier requests have been completed or timed out."
        )

      opt[Boolean]("cookie-secure")
        .action((x, c) => c.copy(cookieSecure = x))
        .text(
          "Enable the Secure attribute on the cookie that stores the token. Defaults to true. Only disable this for testing and development purposes."
        )

      opt[Long]("login-request-timeout")
        .action((x, c) => c.copy(loginTimeout = FiniteDuration(x, duration.SECONDS)))
        .text(
          "Login request timeout. Requests will be evicted if the callback endpoint receives no corresponding request in time."
        )

      opt[String]("oauth-auth")
        .action((x, c) => c.copy(oauthAuth = Uri(x)))
        .required()
        .text("URI of the OAuth2 authorization endpoint")

      opt[String]("oauth-token")
        .action((x, c) => c.copy(oauthToken = Uri(x)))
        .required()
        .text("URI of the OAuth2 token endpoint")

      opt[String]("oauth-auth-template")
        .action((x, c) => c.copy(oauthAuthTemplate = Some(Paths.get(x))))
        .text("OAuth2 authorization request Jsonnet template")

      opt[String]("oauth-token-template")
        .action((x, c) => c.copy(oauthTokenTemplate = Some(Paths.get(x))))
        .text("OAuth2 token request Jsonnet template")

      opt[String]("oauth-refresh-template")
        .action((x, c) => c.copy(oauthRefreshTemplate = Some(Paths.get(x))))
        .text("OAuth2 refresh request Jsonnet template")

      opt[String]("id")
        .hidden()
        .action((x, c) => c.copy(clientId = x))
        .withFallback(() => sys.env.getOrElse("DAML_CLIENT_ID", ""))
        .validate(x =>
          if (x.isEmpty) failure("Environment variable DAML_CLIENT_ID must not be empty")
          else success
        )

      opt[String]("secret")
        .hidden()
        .action((x, c) => c.copy(clientSecret = SecretString(x)))
        .withFallback(() => sys.env.getOrElse("DAML_CLIENT_SECRET", ""))
        .validate(x =>
          if (x.isEmpty) failure("Environment variable DAML_CLIENT_SECRET must not be empty")
          else success
        )

      JwtVerifierConfigurationCli.parse(this)((v, c) => c.copy(tokenVerifier = v))

      checkConfig { cfg =>
        if (cfg.tokenVerifier == null)
          Left("You must specify one of the --auth-jwt-* flags for token verification.")
        else
          Right(())
      }

      help("help").text("Print this usage text")

      note("""
        |Environment variables:
        |  DAML_CLIENT_ID      The OAuth2 client-id - must not be empty
        |  DAML_CLIENT_SECRET  The OAuth2 client-secret - must not be empty
        """.stripMargin)
    }
}
