// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.{Get, Put}

/** A nullary typeclass of incompatible JDBC operations and settings, selected by
  * the options passed to json-api at startup.
  *
  * We currently use the fact that it is nonsensical to run json-api against more
  * than one JDBC driver ''in the image'' as justification to make this typeclass
  * nullary.  If that changes in the future, a phantom type parameter should be
  * introduced so as to distinguish instances by type.
  */
final class SupportedJdbcDriver(
    label: String,
    private[http] val queries: Queries,
    private[http] val retrySqlStates: Set[String],
)(implicit
    private[http] val gvs: Get[Vector[String]],
    private[http] val pvs: Put[Vector[String]],
    private[http] val pls: Put[List[String]],
    private[http] val pas: Put[Array[String]],
) {
  override def toString = s"SupportedJdbcDriver($label)"
}

object SupportedJdbcDriver {
  val Postgres: SupportedJdbcDriver = {
    import doobie.postgres.implicits._
    import doobie.postgres.sqlstate.{class23 => postgres_class23}
    new SupportedJdbcDriver(
      label = "PostgreSQL",
      queries = Queries.Postgres,
      retrySqlStates =
        Set(postgres_class23.UNIQUE_VIOLATION.value, ContractDao.StaleOffsetException.SqlState),
    )
  }
}
