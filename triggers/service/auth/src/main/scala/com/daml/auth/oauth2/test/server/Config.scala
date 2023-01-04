// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import java.time.Clock

import com.daml.ports.Port

case class Config(
    // Port the authorization server listens on
    port: Port,
    // Ledger ID of issued tokens
    ledgerId: String,
    // Secret used to sign JWTs
    jwtSecret: String,
    // Use the provided clock instead of system time for token generation.
    clock: Option[Clock],
    // produce user tokens instead of claim tokens
    yieldUserTokens: Boolean,
)

object Config {
  private val Empty =
    Config(
      port = Port.Dynamic,
      ledgerId = null,
      jwtSecret = null,
      clock = None,
      yieldUserTokens = false,
    )

  def parseConfig(args: collection.Seq[String]): Option[Config] =
    configParser.parse(args, Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val configParser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("oauth-test-server") {
      head("OAuth2 TestServer")

      opt[Int]("port")
        .action((x, c) => c.copy(port = Port(x)))
        .required()
        .text("Port to listen on")

      opt[String]("ledger-id")
        .action((x, c) => c.copy(ledgerId = x))

      opt[String]("secret")
        .action((x, c) => c.copy(jwtSecret = x))

      opt[Unit]("yield-user-tokens")
        .optional()
        .action((_, c) => c.copy(yieldUserTokens = true))

      help("help").text("Print this usage text")
    }
}
