// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.{Get, Put}

class SupportedJdbcDriver(val retrySqlStates: Set[String])(implicit
    val gvs: Get[Vector[String]],
    val pvs: Put[Vector[String]],
    val pls: Put[List[String]],
    val pas: Put[Array[String]],
)

object SupportedJdbcDriver {
  val Postgres: SupportedJdbcDriver = {
    import doobie.postgres.implicits._
    import doobie.postgres.sqlstate.{class23 => postgres_class23}
    new SupportedJdbcDriver(
      retrySqlStates =
        Set(postgres_class23.UNIQUE_VIOLATION.value, ContractDao.StaleOffsetException.SqlState)
    )
  }
}
