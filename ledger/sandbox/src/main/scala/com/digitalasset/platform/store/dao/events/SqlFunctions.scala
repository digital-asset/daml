// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.EventsTableQueries.format

trait SqlFunctions {
  def arrayIntersectionWhereClause(arrayColumn: String, party: Party): String =
    arrayIntersectionWhereClause(arrayColumn: String, Set(party))

  def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String

  def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String
}

object SqlFunctions {
  def arrayIntersection(a: Array[String], b: Array[String]): Array[String] =
    varcharArraysIntersection(a, b)

  def varcharArraysIntersection(a: Array[String], b: Array[String]): Array[String] =
    a.toSet.intersect(b.toSet).toArray

  def doVarcharArraysIntersect(a: Array[String], b: Array[String]): Boolean =
    a.toSet.intersect(b.toSet).nonEmpty

  def apply(dbType: DbType): SqlFunctions = dbType match {
    case DbType.Postgres => PostgresSqlFunctions
    case DbType.H2Database => H2SqlFunctions
  }

  object PostgresSqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      s"da_do_varchar_arrays_intersect($arrayColumn, array[${format(parties)}])"

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"da_varchar_arrays_intersection($arrayColumn, array[${format(parties)}])"
  }

  object H2SqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      s"da_do_varchar_arrays_intersect($arrayColumn, array[${format(parties)}])"

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"da_varchar_arrays_intersection($arrayColumn, array[${format(parties)}])"
  }
}
