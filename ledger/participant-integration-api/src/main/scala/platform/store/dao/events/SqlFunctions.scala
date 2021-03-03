// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.EventsTableQueries.format

private[dao] trait SqlFunctions {
  def arrayIntersectionWhereClause(arrayColumn: String, party: Party): String =
    arrayIntersectionWhereClause(arrayColumn: String, Set(party))

  def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String

  def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String
}

private[dao] object SqlFunctions {
  def arrayIntersection(a: Array[String], b: Array[String]): Array[String] =
    a.toSet.intersect(b.toSet).toArray

  def apply(dbType: DbType): SqlFunctions = dbType match {
    case DbType.Postgres => PostgresSqlFunctions
    case DbType.H2Database => H2SqlFunctions
  }

  object PostgresSqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      s"$arrayColumn::text[] && array[${format(parties)}]::text[]"

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"array(select unnest($arrayColumn) intersect select unnest(array[${format(parties)}]))"
  }

  object H2SqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      if (parties.isEmpty)
        "false"
      else
        parties.view.map(p => s"array_contains($arrayColumn, '$p')").mkString("(", " or ", ")")

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"array_intersection($arrayColumn, array[${format(parties)}])"
  }
}
