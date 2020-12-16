// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import akka.http.scaladsl.model.Uri
import com.daml.jwt.{JwtVerifierBase, JwtVerifierConfigurationCli}
import com.daml.ports.Port

case class Config(
    // Port the middleware listens on
    port: Port,
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
  private val Empty =
    Config(
      port = Port.Dynamic,
      oauthAuth = null,
      oauthToken = null,
      clientId = null,
      clientSecret = null,
      tokenVerifier = null)

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
