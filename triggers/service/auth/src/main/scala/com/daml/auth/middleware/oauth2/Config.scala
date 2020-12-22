// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import akka.http.scaladsl.model.Uri
import com.daml.jwt.{JwtVerifierBase, JwtVerifierConfigurationCli}
import com.daml.ports.Port

import scala.concurrent.duration
import scala.concurrent.duration.{Duration, FiniteDuration}

case class Config(
    // Port the middleware listens on
    port: Port,
    // The URI to which the OAuth2 server will redirect after a completed login flow.
    // Must map to the `/cb` endpoint of the auth middleware.
    callbackUri: Option[Uri],
    maxLoginRequests: Long,
    loginTimeout: Duration,
    // OAuth2 server endpoints
    oauthAuth: Uri,
    oauthToken: Uri,
    // OAuth2 client properties
    clientId: String,
    clientSecret: String,
    // Token verification
    tokenVerifier: JwtVerifierBase,
)

object Config {
  val DefaultMaxLoginRequests: Long = 10000
  val DefaultLoginTimeout: Duration = FiniteDuration(1, duration.MINUTES)

  private val Empty =
    Config(
      port = Port.Dynamic,
      callbackUri = None,
      maxLoginRequests = DefaultMaxLoginRequests,
      loginTimeout = DefaultLoginTimeout,
      oauthAuth = null,
      oauthToken = null,
      clientId = null,
      clientSecret = null,
      tokenVerifier = null
    )

  def parseConfig(args: Seq[String]): Option[Config] =
    configParser.parse(args, Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val configParser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("oauth-middleware") {
      head("OAuth2 Middleware")

      opt[Int]("port")
        .action((x, c) => c.copy(port = Port(x)))
        .required()
        .text("Port to listen on")

      opt[String]("callback")
        .action((x, c) => c.copy(callbackUri = Some(Uri(x))))
        .text("URI to the auth middleware's callback endpoint `/cb`. By default constructed from the incoming login request.")

      opt[Long]("max-pending-login-requests")
        .action((x, c) => c.copy(maxLoginRequests = x))
        .text(
          "Maximum number of simultaneously pending login requests. Earlier requests will be evicted first when exceeded.")

      opt[Long]("login-request-timeout")
        .action((x, c) => c.copy(loginTimeout = FiniteDuration(x, duration.SECONDS)))
        .text("Login request timeout. Requests will be evicted if the callback endpoint receives no corresponding request in time.")

      opt[String]("oauth-auth")
        .action((x, c) => c.copy(oauthAuth = Uri(x)))
        .required()
        .text("URI of the OAuth2 authorization endpoint")

      opt[String]("oauth-token")
        .action((x, c) => c.copy(oauthToken = Uri(x)))
        .required()
        .text("URI of the OAuth2 token endpoint")

      opt[String]("id").hidden
        .action((x, c) => c.copy(clientId = x))
        .withFallback(() => sys.env.getOrElse("DAML_CLIENT_ID", ""))
        .validate(x =>
          if (x.isEmpty) failure("Environment variable DAML_CLIENT_ID must not be empty")
          else success)

      opt[String]("secret").hidden
        .action((x, c) => c.copy(clientSecret = x))
        .withFallback(() => sys.env.getOrElse("DAML_CLIENT_SECRET", ""))
        .validate(x =>
          if (x.isEmpty) failure("Environment variable DAML_CLIENT_SECRET must not be empty")
          else success)

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
