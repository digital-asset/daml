// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
        val expectedPostgresVersions = NonEmpty(Seq, 11, 12, 13, 14, 15)
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
        either.leftMap(DbMigrations.DatabaseVersionError)

      case Profile.Oracle(jdbc) =>
        def checkOracleVersion(): Either[String, Unit] = {

          val expectedOracleVersion = 19
          val expectedOracleVersionPrefix =
            " 19." // Leading whitespace is intentional, see the example bannerString

          // See https://docs.oracle.com/en/database/oracle/oracle-database/18/refrn/V-VERSION.html
          val oracleVersionQuery = sql"select banner from v$$version".as[String].headOption
          val stringO = timeouts.network.await(functionFullName)(db.run(oracleVersionQuery))
          stringO match {
            case Some(bannerString) =>
              // An example `bannerString` is "Oracle Database 18c Express Edition Release 18.0.0.0.0 - Production"
              if (bannerString.contains(expectedOracleVersionPrefix)) {
                loggingContext.debug(
                  s"Check for oracle version $expectedOracleVersion passed: using $bannerString"
                )
                Right(())
              } else {
                Left(s"Expected Oracle version $expectedOracleVersion but got $bannerString")
              }
            case None =>
              Left(s"Database version check failed: could not read Oracle version")
          }
        }

        // Checks that the NLS parameter `param` is set to one of the `expected` strings
        // - The DB setting must be set
        // - The session setting may be empty
        def checkNlsParameter(
            param: String,
            expected: Seq[String],
        ): Either[String, Unit] = {
          def prettyExpected: String =
            if (expected.sizeIs == 1) expected(0)
            else s"one of ${expected.mkString(", ")}"

          loggingContext.debug(s"Checking NLS parameter $param")

          @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
          val queryDbSetting =
            sql"SELECT value from nls_database_parameters where parameter=$param"
              .as[String]
              .headOption
          val dbSettingO =
            timeouts.network.await(functionFullName + s"-database-$param")(db.run(queryDbSetting))

          @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
          val querySessionSetting =
            sql"SELECT value from nls_session_parameters where parameter=$param"
              .as[String]
              .headOption
          val sessionSettingO = timeouts.network.await(functionFullName + s"-session-$param")(
            db.run(querySessionSetting)
          )

          for {
            // Require to find the setting for the database, but leave it optional for the session
            dbSetting <- dbSettingO.toRight(
              s"Oracle NLS database parameter $param is not set, but should be $prettyExpected"
            )
            _ <- Either.cond(
              expected.contains(dbSetting.toUpperCase),
              loggingContext.debug(s"NLS database parameter $param is set to $dbSetting"),
              s"Oracle NLS database parameter $param is $dbSetting, but should be $prettyExpected",
            )

            _ <- sessionSettingO.fold(
              Either.right[String, Unit](
                loggingContext.debug(s"NLS session parameter $param is unset")
              )
            ) { sessionSetting =>
              Either.cond(
                expected.contains(sessionSetting.toUpperCase),
                loggingContext.debug(s"NLS session parameter $param is set to $sessionSetting"),
                s"Oracle NLS session parameter $param is $sessionSetting, but should be $prettyExpected",
              )
            }
          } yield ()
        }

        // Check the NLS settings of the database so that Oracle uses the expected encodings and collations for
        // string fields in tables.
        def checkOracleNlsSetting(): Either[String, Unit] =
          for {
            _ <- checkNlsParameter("NLS_CHARACTERSET", Seq("AL32UTF8"))
            _ <- checkNlsParameter("NLS_NCHAR_CHARACTERSET", Seq("AL32UTF8", "AL16UTF16"))
            _ <- checkNlsParameter("NLS_SORT", Seq("BINARY"))
            _ <- checkNlsParameter("NLS_COMP", Seq("BINARY"))
          } yield ()

        for {
          _ <- checkOracleVersion().leftMap(DbMigrations.DatabaseVersionError)
          _ <- checkOracleNlsSetting().leftMap(DbMigrations.DatabaseConfigError)
        } yield ()
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
