// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.resource.DbStorage.Profile
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton

object DbStringEncodingCheck extends HasLoggerName {

  val UTF8Encoding = "UTF8"

  def apply(
      timeouts: ProcessingTimeout,
      dbConfig: DbConfig,
      db: Database,
  )(implicit
      loggingContext: NamedLoggingContext
  ): EitherT[UnlessShutdown, DbMigrations.DatabaseError, Unit] = {
    loggingContext.debug(s"Performing string-encoding checks")

    DbStorage.profile(dbConfig) match {
      case Profile.Postgres(_) =>
        // Block on the query result, because `withDb` does not support running functions that return a
        // future (at the time of writing).
        timeouts.network
          .await(functionFullName)(
            db.run(
              // See https://www.postgresql.org/docs/current/sql-show.html
              sql"show server_encoding".as[String]
            )
          )
          .headOption
          .toRight(left = DbMigrations.DatabaseError(s"Could not read Postgres server encoding"))
          .map(encodingString =>
            if (encodingString != UTF8Encoding)
              loggingContext.error(
                s"Expected Postgres server character set encoding to be set to $UTF8Encoding to be able to store every UTF8 string possible. Instead observing $encodingString server encoding. This could lead to problems as certain UTF8 strings might be impossible to be stored in the database. Setting $UTF8Encoding as the database encoding for canton databases is highly recommended."
              )
          )
          .toEitherT

      case Profile.H2(_) =>
        Either.unit.toEitherT
    }
  }
}
