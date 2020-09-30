// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.http.scaladsl.model.Uri
import com.daml.ports.Port

private[middleware] case class Config(
    // Port the middleware listens on
    port: Port,
    // Uri of the OAuth2 server
    oauthUri: Uri,
    // OAuth2 client properties
    clientId: String,
    clientSecret: String,
)

private[middleware] object Config {
  private val Empty =
    Config(port = Port.Dynamic, oauthUri = null, clientId = null, clientSecret = null)

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

      opt[String]("oauth-uri")
        .action((x, c) => c.copy(oauthUri = Uri(x)))
        .required()
        .text("URI of the OAuth2 server")

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

      help("help").text("Print this usage text")

      note("""
        |Environment variables:
        |  DAML_CLIENT_ID      The OAuth2 client-id - must not be empty
        |  DAML_CLIENT_SECRET  The OAuth2 client-secret - must not be empty
        """.stripMargin)
    }
}
