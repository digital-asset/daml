// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

/** A nullary typeclass of incompatible JDBC operations and settings, selected by
  * the options passed to json-api at startup.
  *
  * We currently use the fact that it is nonsensical to run json-api against more
  * than one JDBC driver ''in the image'' as justification to make this typeclass
  * nullary.  If that changes in the future, a phantom type parameter should be
  * introduced so as to distinguish instances by type.
  */
sealed abstract class SupportedJdbcDriver private (
    label: String,
    private[http] val retrySqlStates: Set[String],
) {
  type SqlInterpol
  private[http] val queries: Queries.Aux[SqlInterpol]
  private[http] implicit val ipol: SqlInterpol
  override def toString = s"SupportedJdbcDriver($label)"
}

object SupportedJdbcDriver {
  private final class Instance[SI](
      label: String,
      override val queries: Queries.Aux[SI],
      retrySqlStates: Set[String],
  )(implicit override val ipol: SI)
      extends SupportedJdbcDriver(label, retrySqlStates) {
    type SqlInterpol = SI
  }

  val Postgres: SupportedJdbcDriver = {
    import doobie.postgres.implicits._
    import doobie.postgres.sqlstate.{class23 => postgres_class23}
    implicit val ipol: Queries.Postgres.SqlInterpol = new Queries.SqlInterpolation.StringArray()
    new Instance(
      label = "PostgreSQL",
      queries = Queries.Postgres,
      retrySqlStates =
        Set(postgres_class23.UNIQUE_VIOLATION.value, ContractDao.StaleOffsetException.SqlState),
    )
  }

  val Oracle: SupportedJdbcDriver = {
    implicit val ipol: Queries.Oracle.SqlInterpol = new Queries.SqlInterpolation.Unused()
    new Instance(
      label = "Oracle",
      queries = Queries.Oracle,
      // all oracle class 23 errors yield 23000; if we want to check for *unique*
      // violation specifically we'll have to look at something other than the SQLState.
      // All other class 23 errors indicate a bug, which should exhaust the retry loop anyway
      retrySqlStates = Set("23000", ContractDao.StaleOffsetException.SqlState),
    )
  }
}
