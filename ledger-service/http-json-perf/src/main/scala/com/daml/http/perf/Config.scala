// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import com.daml.jwt.JwtDecoder
import com.daml.jwt.domain.Jwt
import com.daml.ports.Port
import scopt.RenderingMode

private[perf] final case class Config(
    jsonApiHost: String,
    jsonApiPort: Port,
    scenario: String,
    jwt: Jwt,
    packageId: Option[String],
) {
  override def toString: String =
    s"Config(jsonApiHost=${this.jsonApiHost}, " +
      s"jsonApiPort=${this.jsonApiPort.value: Int}, " +
      s"scenario=${this.scenario}, " +
      s"jwt=..., " + // don't print the JWT
      s"packageId=${this.packageId.toString})"
}

private[perf] object Config {
  val Empty =
    Config(
      jsonApiHost = "",
      jsonApiPort = Port.Dynamic,
      scenario = "",
      jwt = Jwt(""),
      packageId = None)

  def parseConfig(args: Seq[String]): Option[Config] =
    configParser.parse(args, Config.Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private val configParser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("http-json-perf-binary") {
      override def renderingMode: RenderingMode = RenderingMode.OneColumn

      head("JSON API Perf Test Tool")

      help("help").text("Print this usage text")

      opt[String]("json-api-host")
        .action((x, c) => c.copy(jsonApiHost = x))
        .required()
        .text("JSON API host name or IP address")

      opt[Int]("json-api-port")
        .action((x, c) => c.copy(jsonApiPort = Port(x)))
        .required()
        .validate(validatePort)
        .text("JSON API port number")

      opt[String]("scenario")
        .action((x, c) => c.copy(scenario = x))
        .required()
        .text("Performance test scenario to run")

      opt[String]("jwt")
        .action((x, c) => c.copy(jwt = Jwt(x)))
        .required()
        .validate(validateJwt)
        .text("JWT token to use when connecting to JSON API")

      opt[String]("package-id")
        .action((x, c) => c.copy(packageId = Some(x)))
        .optional()
        .text("Optional package ID to specify in the commands sent to JSON API")
    }

  private def validatePort(p: Int): Either[String, Unit] =
    Port.validate(p).toEither.left.map(x => x.getMessage)

  private def validateJwt(s: String): Either[String, Unit] = {
    import scalaz.syntax.show._

    JwtDecoder
      .decode(Jwt(s))
      .bimap(
        error => error.shows,
        _ => ()
      )
      .toEither
  }
}
