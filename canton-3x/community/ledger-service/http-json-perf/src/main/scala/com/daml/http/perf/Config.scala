// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import java.io.File

import scopt.RenderingMode

import Config.QueryStoreIndex
import com.daml.http.dbbackend.ContractDao.supportedJdbcDriverNames
import com.daml.runtime.JdbcDrivers.availableJdbcDriverNames

import scala.concurrent.duration.{Duration, FiniteDuration}

private[perf] final case class Config[+S](
    scenario: S,
    dars: List[File],
    reportsDir: File,
    maxDuration: Option[FiniteDuration],
    queryStoreIndex: QueryStoreIndex,
) {
  override def toString: String =
    s"Config(" +
      s"scenario=${this.scenario}" +
      s", dars=${dars: List[File]}, " +
      s", jwt=..." + // don't print the JWT
      s", reportsDir=${reportsDir: File}" +
      s", maxDuration=${this.maxDuration: Option[FiniteDuration]}" +
      s", queryStoreIndex=${this.queryStoreIndex: QueryStoreIndex}" +
      ")"
}

private[perf] object Config {
  val Empty =
    Config[String](
      scenario = "",
      dars = List.empty,
      reportsDir = new File(""),
      maxDuration = None,
      queryStoreIndex = QueryStoreIndex.No,
    )

  def parseConfig(args: collection.Seq[String]): Option[Config[String]] =
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

      opt[QueryStoreIndex]("query-store-index")
        .action((x, c) => c.copy(queryStoreIndex = x))
        .optional()
        .text(
          s"Enables JSON API query store index ${QueryStoreIndex.allowedHelp}. Default is no, disabled."
        )

      opt[File]("reports-dir")
        .action((x, c) => c.copy(reportsDir = x))
        .optional()
        .text("Directory where reports generated. If not set, reports will not be generated.")

      opt[Duration]("max-duration")
        .action((x, c) => c.copy(maxDuration = Some(FiniteDuration(x.length, x.unit))))
        .optional()
        .text(
          s"Optional maximum perf test duration. Default value infinity. Examples: 500ms, 5s, 10min, 1h, 1d."
        )
    }

  sealed abstract class QueryStoreIndex extends Product with Serializable
  object QueryStoreIndex {
    case object No extends QueryStoreIndex
    case object Postgres extends QueryStoreIndex
    case object Oracle extends QueryStoreIndex

    val names: Map[String, QueryStoreIndex] = Map("no" -> No, "postgres" -> Postgres) ++ (
      if (supportedJdbcDriverNames(availableJdbcDriverNames)("oracle.jdbc.OracleDriver"))
        Seq("oracle" -> Oracle)
      else Seq.empty
    )

    private[Config] val allowedHelp = names.keys.mkString("{", ", ", "}")

    implicit val scoptRead: scopt.Read[QueryStoreIndex] = scopt.Read.reads { s =>
      names.getOrElse(
        s.toLowerCase(java.util.Locale.ROOT),
        throw new IllegalArgumentException(
          s"$s is not a query store index; only $allowedHelp allowed"
        ),
      )
    }
  }
}
