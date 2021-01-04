// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import java.io.File

import com.daml.jwt.JwtDecoder
import com.daml.jwt.domain.Jwt
import scalaz.{Applicative, Traverse}
import scopt.RenderingMode

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.higherKinds

private[perf] final case class Config[+S](
    scenario: S,
    dars: List[File],
    jwt: Jwt,
    reportsDir: File,
    maxDuration: Option[FiniteDuration],
    queryStoreIndex: Boolean,
) {
  override def toString: String =
    s"Config(" +
      s"scenario=${this.scenario}" +
      s", dars=${dars: List[File]}, " +
      s", jwt=..." + // don't print the JWT
      s", reportsDir=${reportsDir: File}" +
      s", maxDuration=${this.maxDuration: Option[FiniteDuration]}" +
      s", queryStoreIndex=${this.queryStoreIndex: Boolean}" +
      ")"
}

private[perf] object Config {
  val Empty =
    Config[String](
      scenario = "",
      dars = List.empty,
      jwt = Jwt(""),
      reportsDir = new File(""),
      maxDuration = None,
      queryStoreIndex = false)

  implicit val configInstance: Traverse[Config] = new Traverse[Config] {
    override def traverseImpl[G[_]: Applicative, A, B](fa: Config[A])(
        f: A => G[B]): G[Config[B]] = {
      import scalaz.syntax.functor._
      f(fa.scenario).map(b => Empty.copy(scenario = b))
    }
  }

  def parseConfig(args: Seq[String]): Option[Config[String]] =
    configParser.parse(args, Config.Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private val configParser: scopt.OptionParser[Config[String]] =
    new scopt.OptionParser[Config[String]]("http-json-perf-binary") {
      override def renderingMode: RenderingMode = RenderingMode.OneColumn

      head("JSON API Perf Test Tool")

      help("help").text("Print this usage text")

      opt[String]("scenario")
        .action((x, c) => c.copy(scenario = x))
        .required()
        .text("Performance test scenario to run.")

      opt[Seq[File]]("dars")
        .action((x, c) => c.copy(dars = x.toList))
        .required()
        .text("DAR files to pass to Sandbox.")

      opt[String]("jwt")
        .action((x, c) => c.copy(jwt = Jwt(x)))
        .required()
        .validate(validateJwt)
        .text("JWT token to use when connecting to JSON API.")

      opt[Boolean]("query-store-index")
        .action((x, c) => c.copy(queryStoreIndex = x))
        .optional()
        .text("Enables JSON API query store index. Default is false, disabled.")

      opt[File]("reports-dir")
        .action((x, c) => c.copy(reportsDir = x))
        .optional()
        .text("Directory where reports generated. If not set, reports will not be generated.")

      opt[Duration]("max-duration")
        .action((x, c) => c.copy(maxDuration = Some(FiniteDuration(x.length, x.unit))))
        .optional()
        .text(s"Optional maximum perf test duration. Default value infinity. Examples: 500ms, 5s, 10min, 1h, 1d.")
    }

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
