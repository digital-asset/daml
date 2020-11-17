// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import java.time.Clock

import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ports.Port

case class Config(
    // Port the authorization server listens on
    port: Port,
    // Ledger ID of issued tokens
    ledgerId: String,
    // Application ID of issued tokens
    applicationId: Option[String],
    // Secret used to sign JWTs
    jwtSecret: String,
    // Only authorize requests for these parties, if set.
    parties: Option[Set[Party]],
    // Use the provided clock instead of system time for token generation.
    clock: Option[Clock],
)

object Config {
  private val Empty =
    Config(
      port = Port.Dynamic,
      ledgerId = null,
      applicationId = None,
      jwtSecret = null,
      parties = None,
      clock = None)

  def parseConfig(args: Seq[String]): Option[Config] =
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

      opt[String]("application-id")
        .action((x, c) => c.copy(applicationId = Some(x)))

      opt[String]("secret")
        .action((x, c) => c.copy(jwtSecret = x))

      opt[Seq[String]]("parties")
        .action((x, c) => c.copy(parties = Some(Party.subst(x).toSet)))
        .text("Only authorize requests for these parties")

      help("help").text("Print this usage text")
    }
}
