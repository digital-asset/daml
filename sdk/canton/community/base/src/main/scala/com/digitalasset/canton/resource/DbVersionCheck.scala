// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.util.LoggerUtil
import org.slf4j.event.Level
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

import scala.util.Try

object DbVersionCheck extends HasLoggerName {

  def dbVersionCheck(
      timeouts: ProcessingTimeout,
      standardConfig: Boolean,
      dbConfig: DbConfig,
  )(implicit
      loggingContext: NamedLoggingContext
  ): Database => Either[DbMigrations.Error, Unit] = { db =>
    loggingContext.debug(s"Performing version checks")
    val profile = DbStorage.profile(dbConfig)
    val either: Either[DbMigrations.Error, Unit] = profile match {

      case Profile.Postgres(jdbc) =>
        val expectedPostgresVersions = NonEmpty(Seq, 13, 14, 15, 16)
        val expectedPostgresVersionsStr =
          s"${(expectedPostgresVersions.dropRight(1)).mkString(", ")}, or ${expectedPostgresVersions
              .takeRight(1)
              .mkString("")}"
        val maxPostgresVersion = expectedPostgresVersions.max1

        // See https://www.postgresql.org/docs/9.1/sql-show.html
        val query = sql"show server_version".as[String]
        // Block on the query result, because `withDb` does not support running functions that return a
        // future (at the time of writing).
        val vector = timeouts.network.await(functionFullName)(db.run(query))
        val stringO = vector.headOption
        val either = for {
          versionString <- stringO.toRight(left = s"Could not read Postgres version")
          // An example `versionString` is 12.9 (Debian 12.9-1.pgdg110+1)
          majorVersion <- versionString
            .split('.')
            .headOption
            .toRight(left =
              s"Could not parse Postgres version string $versionString. Are you using the recommended Postgres version 11 ?"
            )
            .flatMap(str =>
              Try(str.toInt).toEither.leftMap(exn =>
                s"Exception in parsing Postgres version string $versionString: $exn"
              )
            )
          _unit <- {
            if (expectedPostgresVersions.contains(majorVersion)) Right(())
            else if (majorVersion > maxPostgresVersion) {
              val level = if (standardConfig) Level.WARN else Level.INFO
              LoggerUtil.logAtLevel(
                level,
                s"Expected Postgres version $expectedPostgresVersionsStr but got higher version $versionString",
              )
              Right(())
            } else
              Left(
                s"Expected Postgres version $expectedPostgresVersionsStr but got lower version $versionString"
              )
          }
        } yield ()
        either.leftMap(DbMigrations.DatabaseVersionError.apply)

      case Profile.H2(_) =>
        // We don't perform version checks for H2
        Right(())
    }
    if (standardConfig) either
    else
      either.leftFlatMap { error =>
        loggingContext.info(error.toString)
        Right(())
      }
  }
}
