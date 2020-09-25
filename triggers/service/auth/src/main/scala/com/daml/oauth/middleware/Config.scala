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
    Config(
      port = Port.Dynamic,
      oauthUri = Uri().withScheme("http"),
      clientId = null,
      clientSecret = null)

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

      opt[String]("oauth-host")
        .action((x, c) => c.copy(oauthUri = c.oauthUri.withHost(x)))
        .required()
        .text("Hostname of the OAuth2 server")

      opt[Int]("oauth-port")
        .action((x, c) => c.copy(oauthUri = c.oauthUri.withPort(x)))
        .required()
        .text("Port of the OAuth2 server")

      opt[String]("id")
        .action((x, c) => c.copy(clientId = x))
        .required()
        .text("OAuth2 client id")

      opt[String]("secret")
        .action((x, c) => c.copy(clientSecret = x))
        .required()
        .text("OAuth2 client secret")

      help("help").text("Print this usage text")
    }
}
