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

  def toArray(value: String): String

  def limitClause(numRows: Int): String

  def groupByIncludingBinaryAndArrayColumns(cols: Seq[String]): String =
    s"group by (${cols.mkString(", ")})"
}

private[dao] object SqlFunctions {
  def arrayIntersection(a: Array[String], b: Array[String]): Array[String] =
    a.toSet.intersect(b.toSet).toArray

  def apply(dbType: DbType): SqlFunctions = dbType match {
    case DbType.Postgres => PostgresSqlFunctions
    case DbType.H2Database => H2SqlFunctions
    case DbType.Oracle => OracleSqlFunctions
  }

  object PostgresSqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      s"$arrayColumn && array[${format(parties)}]::varchar[]"

    override def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"array(select unnest($arrayColumn) intersect select unnest(array[${format(parties)}]))"

    override def toArray(value: String) = s"array['$value']"

    override def limitClause(numRows: Int) = s"limit $numRows"
  }

  object H2SqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      if (parties.isEmpty)
        "false"
      else
        parties.view.map(p => s"array_contains($arrayColumn, '$p')").mkString("(", " or ", ")")

    override def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"array_intersection($arrayColumn, array[${format(parties)}])"

    override def toArray(value: String) = s"array['$value']"

    override def limitClause(numRows: Int) = s"limit $numRows"
  }

  object OracleSqlFunctions extends SqlFunctions {
    // TODO https://github.com/digital-asset/daml/issues/9493
    // This is likely extremely inefficient due to the multiple full tablescans on unindexed varray column
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      parties
        .map(party => s"('$party') IN (SELECT * FROM TABLE($arrayColumn))")
        .mkString("(", " or ", ")")

    override def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"CAST(MULTISET(select unique $arrayColumn.* FROM TABLE($arrayColumn) $arrayColumn intersect select * from TABLE(VARCHAR_ARRAY(${format(parties)}))) as VARCHAR_ARRAY)"

    override def toArray(value: String) = s"VARCHAR_ARRAY('$value')"

    override def limitClause(numRows: Int) = s"fetch next $numRows rows only"

    // Oracle cannot group by including columns which are BLOB or VARRAY
    // TODO https://github.com/digital-asset/daml/issues/9493
    override def groupByIncludingBinaryAndArrayColumns(cols: Seq[String]): String = ""
  }
}
