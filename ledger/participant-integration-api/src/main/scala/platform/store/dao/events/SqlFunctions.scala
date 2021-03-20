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

  def equalsClause(left: String): String

  def equalsClauseEnd(): String

  def greaterThanClause(left: String, right: Any): String

  def greaterThanClauseStart(left: String): String

  def greaterThanClauseEnd(): String

  def lessThanOrEqualToClause(left: String, right: Any): String
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

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"array(select unnest($arrayColumn) intersect select unnest(array[${format(parties)}]))"

    def toArray(value: String) = s"array['$value']"

    def limitClause(numRows: Int) = s"limit $numRows"

    def equalsClause(left: String) = s"$left = "

    def equalsClauseEnd(): String = ""

    def greaterThanClause(left: String, right: Any) = s"$left > $right"

    def greaterThanClauseStart(left: String): String = s"$left > "

    def greaterThanClauseEnd(): String = ""

    def lessThanOrEqualToClause(left: String, right: Any) = s"$left <= $right"
  }

  object H2SqlFunctions extends SqlFunctions {
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      if (parties.isEmpty)
        "false"
      else
        parties.view.map(p => s"array_contains($arrayColumn, '$p')").mkString("(", " or ", ")")

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"array_intersection($arrayColumn, array[${format(parties)}])"

    def toArray(value: String) = s"array['$value']"

    def limitClause(numRows: Int) = s"limit $numRows"

    def equalsClause(left: String) = s"$left = "

    def equalsClauseEnd(): String = ""

    def greaterThanClause(left: String, right: Any) = s"$left > $right"

    def greaterThanClauseStart(left: String): String = s"$left > "

    def greaterThanClauseEnd(): String = ""

    def lessThanOrEqualToClause(left: String, right: Any) = s"$left <= $right"
  }

  //TODO need to properly implement this for Oracle
  object OracleSqlFunctions extends SqlFunctions {
    //TODO BH: this is likely extremely inefficient
    override def arrayIntersectionWhereClause(arrayColumn: String, parties: Set[Party]): String =
      parties
        .map(party => s"('$party') IN (SELECT * FROM TABLE($arrayColumn))")
        .mkString("(", " or ", ")")

    def arrayIntersectionValues(arrayColumn: String, parties: Set[Party]): String =
      s"CAST(MULTISET(select unique $arrayColumn.* FROM TABLE($arrayColumn) $arrayColumn intersect select * from TABLE(VARCHAR_ARRAY(${format(parties)}))) as VARCHAR_ARRAY)"

    def toArray(value: String) = s"VARCHAR_ARRAY('$value')"

    def limitClause(numRows: Int) = s"fetch next $numRows rows only"

    def equalsClause(left: String) = s"DBMS_LOB.compare($left, "

    def equalsClauseEnd() = ") = 0"

    def greaterThanClause(left: String, right: Any) = s"DBMS_LOB.compare($left, $right) = 1"

    def greaterThanClauseStart(left: String): String = s"DBMS_LOB.compare($left, "

    def greaterThanClauseEnd(): String = ") = 1"

    def lessThanOrEqualToClause(left: String, right: Any) =
      s"DBMS_LOB.compare($left, $right) IN (0, -1)"
  }

}
